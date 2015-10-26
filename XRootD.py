# -*- coding: utf-8 -*-
#
# Copyright 2014 Institut für Experimentelle Kernphysik - Karlsruher Institut für Technologie
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

import hf
import time
import os
import numpy as np
from sqlalchemy import *
import json


class XRootD(hf.module.ModuleBase):
    config_keys = {
        'source_url': ('Source File', ''),
        'tier_name': ('Tier to be monitored', ''),
        'attribute': ('Attribute to be plotted (active, finished, avg_trans_rate_p_file)', '')
    }
    config_hint = ''

    table_columns = [Column('tier_name', TEXT),
                     Column('filename_plot', TEXT),
                     Column('attribute', TEXT)
                     ], ['filename_plot']

    subtable_columns = {
        'details': ([
            Column("date", TEXT),
            Column("plot_data", FLOAT)
        ], [])
    }

    def prepareAcquisition(self):
        self.source = hf.downloadService.addDownload(self.config['source_url'])
        self.source_url = self.source.getSourceUrl()
        self.details_list = []

    def extractData(self):
	import matplotlib
	import matplotlib.pyplot as plt
        data = {}
        list_of_details = []
        plot_date = []
        plot_attribute = []
        with open(self.source.getTmpPath(), 'r') as f:
            data_object = json.loads(f.read())
        
        if str(self.config['attribute']) == 'avg_trans_rate_p_file':
            for group in data_object['transfers']:
                if str(group['name']) == self.config['tier_name']:
                    for jobs in group['bins']:
                        details = {}
                        struct_time = time.strptime(jobs['start_time'], "%Y-%m-%dT%H:%M:%S")
                        details['date'] = time.strftime("%Y-%m-%d %H:%M", struct_time)
                        details['plot_data'] = (float(jobs['finished']))/(
                                                    float(jobs['active']))*float(
                                                        jobs['bytes'])/float(
                                                            jobs['active_time'])/1000000.0
                        list_of_details.append(details)
                    break
            data['attribute'] = 'Average transfer rate per file (MB/s)'
        else:    
            for group in data_object['transfers']:
                if str(group['name']) == self.config['tier_name']:
                    for jobs in group['bins']:
                        details = {}
                        struct_time = time.strptime(jobs['start_time'], "%Y-%m-%dT%H:%M:%S")
                        details['date'] = time.strftime("%Y-%m-%d %H:%M", struct_time)
                        details['plot_data'] = float(jobs[self.config['attribute']])
                        list_of_details.append(details)
                    break
            data['attribute'] = '%s transfers' % self.config['attribute']
        
        list_of_details = sorted(list_of_details, key = lambda k: k['date'])
        self.details_list = list_of_details
        data['tier_name'] = self.config['tier_name']
              
        ######################################
        ####PLOT#####################
        
        #Plot-lists
        data_list = []
        datetime_list = []
        
        for job in list_of_details:
            data_list.append(job['plot_data'])
            datetime_list.append(job['date'])
        
        self.plt = plt
        fig = self.plt.figure()
        axis = fig.add_subplot(111)
        ind = np.arange(len(data_list))
        width = 1.0
        max_value = max(data_list)
        if max_value < 4.0:
            scale_value = 0.2
        else:
            scale_value = max_value // 4
        
        p1 = axis.bar(ind, data_list, width, color='blue')
        
        axis.set_position([0.2,0.3,0.75,0.6])
        axis.set_ylabel(data['attribute'])
        axis.set_title(self.config['tier_name'])
        axis.set_xticks(ind+width/2.)
        axis.set_xticklabels(datetime_list, rotation='vertical')
        axis.set_yticks(np.arange(0, max_value + scale_value, scale_value))
        
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + '_xrootd.png'), dpi=60)
        data['filename_plot'] = self.instance_name + '_xrootd.png'
        
        
        return data
        
    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute(
            [dict(parent_id=parent_id, **row) for row in self.details_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        
        details_list = self.subtables['details'].select().where(
            self.subtables['details'].c.parent_id==self.dataset['id']
            ).execute().fetchall()
        details_list = map(lambda x: dict(x), details_list)
        
        data['details'] = sorted(details_list, key = lambda k: k['date'])
        return data

