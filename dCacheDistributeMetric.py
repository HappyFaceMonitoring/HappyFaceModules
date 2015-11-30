# -*- coding: utf-8 -*-
#
# Copyright 2012 Institut für Experimentelle Kernphysik - Karlsruher Institut für Technologie
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import hf, lxml, logging, datetime
import numpy as np
from sqlalchemy import *
from lxml.html import parse
from string import strip
from string import replace

class dCacheDistributeMetric(hf.module.ModuleBase):
    config_keys = {
        'lower_variance_limit': ('variance limit at highest number of files', '0.07'),
        'upper_variance_limit': ('variance limit at lowest number of files', '0.4'),
        'distribute_source': ('link to the distribute imbalance metric source file', 'both||http://ekphappyface.physik.uni-karlsruhe.de/upload/gridka/dcache_distribute_imbalance_metric'),
        'pool_source_xml': ('link to the pool source file', 'both||http://cmsdcacheweb-kit.gridka.de:2288/info/pools'),
    }
    #'categories': ('name of the categories to be extracted, poolname and status will always be generated', 'total,free,precious,removable'),
    config_hint = ''

    table_columns = [
        Column('num_pools', INT),
        Column('filename_plot', TEXT),
    ], ['filename_plot']

    subtable_columns = {
        "too_low": ([
            Column('name', TEXT),
            Column('number_of_files', INT),
            Column('dist_metric', FLOAT),
        ], []),
        "too_high": ([
            Column('name', TEXT),
            Column('number_of_files', INT),
            Column('dist_metric', FLOAT),
        ], []),
    }
    def max_value(self, val):
        return min(1.0, 0.04 + (2.0 * max(0, 1.0 / val))**0.5)
    
    def ideal_value(self, val, num_pools):
        return max(0.0, 1.0 / val - 1.0 / num_pools)**0.5
    
    def topfunc(self, xlist):
            ylist = []
            for item in xlist:
                ylist.append(self.max_value(item))
            return ylist
    
    def lowfunc(self, xlist, num_of_pools):
            ylist =[]
            for item in xlist:
                ylist.append(self.ideal_value(item, num_of_pools))
            return ylist
    
    def prepareAcquisition(self):
        self.lower_variance_limit = self.config['lower_variance_limit']
        self.upper_variance_limit = self.config['upper_variance_limit']
        self.pool_source_url = self.config['pool_source_xml']
        self.pool_source_xml = hf.downloadService.addDownload(self.config['pool_source_xml'])
        self.distribute_source_url = self.config['distribute_source']
        self.distribute_source = hf.downloadService.addDownload(self.config['distribute_source'])
        self.source_url = self.distribute_source.getSourceUrl()
        self.too_high_value_list = []
        self.too_low_value_list = []

    def extractData(self):
        data = {}
        data['status'] = 1
        data['num_pools'] = 0

        #find number of pools
        source_tree = parse(open(self.pool_source_xml.getTmpPath()))
        root = source_tree.getroot()
        #find pools and count if disk-pool
        root = root.findall('.//pool')
        for pool in root:
            groups = pool.findall('.//poolgroupref')
            for group in groups:
                if 'disk-only-pools' in group.attrib['name']:
                    data['num_pools'] += 1
                    break
        
        #extract name an file distribution
        dist_file = open(self.distribute_source.getTmpPath())
        namelist = []
        xlist = []
        ylist = []
        for line in dist_file:
            line = line.split()
            name = str(line[0])
            num_of_files = int(line[-1])
            dist_metric = float(line[1])
            namelist.append(name)
            xlist.append(int(num_of_files))
            ylist.append(float(dist_metric))
            if (float(dist_metric) < (self.ideal_value(num_of_files, data['num_pools'])-0.01)):
                self.too_low_value_list.append({'name': name, 'number_of_files': num_of_files, 'dist_metric': dist_metric})
            elif (float(dist_metric) > self.max_value(num_of_files)):
                self.too_high_value_list.append({'name': name, 'number_of_files': num_of_files, 'dist_metric': dist_metric})
        single_xlist = []
        for number in xlist:
            if float(number) not in single_xlist:
                single_xlist.append(float(number))
        srtd_xlist = sorted(single_xlist + [1e5])
        
        #plotting
	import matplotlib.pyplot as plt
        self.plt = plt
        fig = self.plt.figure()
        axis = fig.add_subplot(111)
        width = 1.0
        p1 = axis.fill_between(srtd_xlist, self.lowfunc(srtd_xlist, data['num_pools']), self.topfunc(srtd_xlist), alpha=0.2, color='green')
        p2 = axis.plot(srtd_xlist, self.lowfunc(srtd_xlist, data['num_pools']), 'g-', label="optimal")
        p3 = axis.plot(srtd_xlist, self.topfunc(srtd_xlist), '-', color='red', label="warning")
        p4 = axis.axhline(0.997, color="DarkRed")  # 0.997 instead of 1.0 to make it visible below 1.0 axis limit
        p5 = axis.plot(xlist, ylist, 'bx')
        axis.set_ylabel('Imbalance Metric $m$')
        axis.set_xlabel('Number of Files')
        axis.set_title('Distribute Imbalance Metric')
        axis.set_xscale('log')
        axis.set_ylim(0, 1.0)
        axis.text(25, 0.9, r"$m(ds) = \sqrt{\sum_{p=0..N_\mathrm{pools}} "\
            r"\left[ (size(ds,p) - size_\mathrm{opt}(ds,p)) / size(ds) \right]^2}}$")
        axis.text(25, 0.8, r"$size_\mathrm{opt}(ds, p) = size(ds) \cdot size(p) / size(all pools)$")
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + '_dist_metric.png'), dpi=100)
        data['filename_plot'] = self.instance_name + '_dist_metric.png'
        
        return data

    def fillSubtables(self, parent_id):
        self.subtables['too_low'].insert().execute([dict(parent_id=parent_id, **row) for row in self.too_low_value_list])
        self.subtables['too_high'].insert().execute([dict(parent_id=parent_id, **row) for row in self.too_high_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        
        too_low_list = self.subtables['too_low'].select().where(self.subtables['too_low'].c.parent_id==self.dataset['id']).execute().fetchall()
        too_low_list = map(lambda x: dict(x), too_low_list)
        data['too_low'] = sorted(too_low_list, key = lambda k: k['number_of_files'])
        
        too_high_list = self.subtables['too_high'].select().where(self.subtables['too_high'].c.parent_id==self.dataset['id']).execute().fetchall()
        too_high_list = map(lambda x: dict(x), too_high_list)
        data['too_high'] = sorted(too_high_list, key = lambda k: k['number_of_files'])
        
        return data
