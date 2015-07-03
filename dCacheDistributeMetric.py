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
            Column('number_of_pools', INT),
        ], []),
    }
    
    def ideal_fun(self, number_list, num_of_pools):
        return_list = {'ideal': [],'with_offset': [],'xlist': []}
        for number in number_list:
            if float(number) not in return_list['xlist']:
                return_list['ideal'].append(float((max(0,(1.0/float(number))-(1.0/float(num_of_pools))))**0.5))
                return_list['xlist'].append(float(number))
        sorted = zip(return_list['xlist'], return_list['ideal'])
        sorted.sort()
        return_list['xlist'], return_list['ideal']= zip(*sorted)
        offset = np.array(np.interp(return_list['xlist'], [min(return_list['xlist']),max(return_list['xlist'])], [0.4, 0.07]))
        return_list['with_offset'] = (np.array(return_list['ideal'])+offset).tolist()
        return return_list

    def prepareAcquisition(self):
        self.lower_variance_limit = self.config['lower_variance_limit']
        self.upper_variance_limit = self.config['upper_variance_limit']
        self.pool_source_url = self.config['pool_source_xml']
        self.pool_source_xml = hf.downloadService.addDownload(self.config['pool_source_xml'])
        self.distribute_source_url = self.config['distribute_source']
        self.distribute_source = hf.downloadService.addDownload(self.config['distribute_source'])
        self.source_url = self.distribute_source.getSourceUrl()
        self.details_db_value_list = []

    def extractData(self):
        data = {}
        data['status'] = 1
        data['num_pools'] = 0

        #find number of pools
        source_tree = parse(open(self.pool_source_xml.getTmpPath()))
        root = source_tree.getroot()
        #find pools andcount if disk-pool
        root = root.findall('.//pool')
        for pool in root:
            groups = pool.findall('.//poolgroupref')
            for group in groups:
                if 'disk-only-pools' in group.attrib['name']:
                    data['num_pools'] += 1
                    break
        #print data['num_pools']
        """
        plt.plot(xlist, ylist,'x')
        plt.plot(sorted(xlist), self.ideal_function(sorted(xlist)),'o')
        plt.xscale('log')
        plt.show()
        """
        ######################################
        ####PLOT#####################
        
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
        
        self.plt = plt
        fig = self.plt.figure()
        axis = fig.add_subplot(111)
        #ind = np.arange(len(xlist))
        width = 1.0
        """max_value = max(ylist)
        if max_value < 4.0:
            scale_value = 0.2
        else:
            scale_value = max_value // 4
        """
        ideal_list = self.ideal_fun(xlist, data['num_pools'])
        #para_list = np.array(np.interp(xlist, [min(xlist),max(xlist)], [0.07, 0.4])).tolist()
        p1 = axis.plot(xlist, ylist, 'bx')
        #p2 = axis.plot(ideal_list['xlist'], ideal_list['ideal'], 'r*')
        p3 = axis.fill_between(ideal_list['xlist'], ideal_list['ideal'], ideal_list['with_offset'], alpha=0.2, color='red')
        #plt.fill_between(xlist, ideal_list , ideal_list, alpha = 0.2)
        
        axis.set_position([0.2,0.3,0.75,0.6])
        axis.set_ylabel('Usage of Pools')
        axis.set_xlabel('Number of Files')
        axis.set_title('Distribute Imbalance Metric')
        #axis.set_xticks(ind+width/2.)
        #axis.set_xticklabels(datetime_list, rotation='vertical')
        #axis.set_yticks(np.arange(0, max_value + scale_value, scale_value))
        axis.set_xscale('log')
        
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + '_dist_metric.png'), dpi=100)
        data['filename_plot'] = self.instance_name + '_dist_metric.png'
        
        return data

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).execute().fetchall()
        details_list = map(lambda x: dict(x), details_list)
 
        return data
