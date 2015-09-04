# -*- coding: utf-8 -*-
#
# Copyright 2012 Institut fÃ¼r Experimentelle Kernphysik - Karlsruher Institut fÃ¼r Technologie
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
import matplotlib.pyplot as plt
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
        'pool_source_xml': ('link to the pool source file', 'both||http://adm-dcache.gridka.de:2286/info/pools'),#both||http://cmsdcacheweb-kit.gridka.de:2288/webadmin/usageinfo
    }
    #'categories': ('name of the categories to be extracted, poolname and status will always be generated', 'total,free,precious,removable'),
    config_hint = ''

    table_columns = [
        Column('num_pools', INT),
        Column('filename_plot', TEXT),
    ], ['filename_plot']

    subtable_columns = {
        "details": ([
            Column('name', TEXT),
            Column('number_of_files', INT),
            Column('dist_metric', FLOAT),
            #Column('number_of_pools', INT),
        ], []),
        "out_of_range": ([
            Column('name', TEXT),
            Column('number_of_files', INT),
            Column('dist_metric', FLOAT),
        ], []),
    }
    def max_value(self, val):
        return (0.04+float(2.0*(max(0,(1.0/float(val)))))**0.5)
    
    def ideal_value(self, val, num_pools):
        return (float((max(0,(1.0/float(val))-(1.0/float(num_pools))))**0.5))
    
    def topfunc(self, xlist):
            ylist = []
            for item in xlist:
                #ylist.append(0.04+float(2.0*(max(0,(1.0/float(item)))))**0.5)
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
        self.details_db_value_list = []
        self.out_of_range_db_value_list = []

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
            name = line[0]
            num_of_files = line[-1]
            dist_metric = line[1]
            namelist.append(name)
            xlist.append(int(num_of_files))
            ylist.append(float(dist_metric))
            self.details_db_value_list.append({'name': name, 'number_of_files': num_of_files, 'dist_metric': dist_metric})
            if not((self.ideal_value(num_of_files, data['num_pools'])-0.01) <= float(dist_metric) <= self.max_value(num_of_files)):
                self.out_of_range_db_value_list.append({'name': name, 'number_of_files': num_of_files, 'dist_metric': dist_metric})
        single_xlist = []
        for number in xlist:
            if float(number) not in single_xlist:
                single_xlist.append(float(number))
        srtd_xlist = sorted(single_xlist)
        
        #plotting
        self.plt = plt
        fig = self.plt.figure()
        axis = fig.add_subplot(111)
        width = 1.0
        p1 = axis.plot(xlist, ylist, 'bx')
        p2 = axis.fill_between(srtd_xlist, self.lowfunc(srtd_xlist, data['num_pools']), self.topfunc(srtd_xlist), alpha=0.2, color='red')
        axis.set_ylabel('Usage of Pools')
        axis.set_xlabel('Number of Files')
        axis.set_title('Distribute Imbalance Metric')
        axis.set_xscale('log')
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + '_dist_metric.png'), dpi=100)
        data['filename_plot'] = self.instance_name + '_dist_metric.png'
        
        return data

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])
        self.subtables['out_of_range'].insert().execute([dict(parent_id=parent_id, **row) for row in self.out_of_range_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        
        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).execute().fetchall()
        details_list = map(lambda x: dict(x), details_list)
        data['details'] = sorted(details_list, key = lambda k: k['number_of_files'])
        
        warn_list = self.subtables['out_of_range'].select().where(self.subtables['out_of_range'].c.parent_id==self.dataset['id']).execute().fetchall()
        warn_list = map(lambda x: dict(x), warn_list)
        data['warn_list'] = sorted(warn_list, key = lambda k: k['number_of_files'])
        
        return data
