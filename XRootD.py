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
from scipy.interpolate import spline

class XRootD(hf.module.ModuleBase):
    config_keys = {
        'source_url': ('Source File', ''),
        'tier_name': ('Tier to be monitored', ''),
        'attribute': ('Choose max. 2 attributes to be plotted, separated by a semicolon. If more than 2 attributes are given, only the first two are considered. (active, finished, avg_trans_rate_p_file)', 'active;avg_trans_rate_p_file')
    }
    config_hint = ''

    table_columns = [Column('tier_name', TEXT),
                     Column('filename_plot', TEXT),
                     Column('attribute', TEXT)
                     ], ['filename_plot']

    subtable_columns = {
        'details': ([
            Column("date", TEXT),
            Column("plot_data", TEXT)
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

        # Considering only the first two attributes given.
        att_list = self.config['attribute'].split(';')[:2]
        att_string = ''

        for group in data_object['transfers']:
            if str(group['name']) == self.config['tier_name']:
                for jobs in group['bins']:
                    details = {}
                    data_string = ''
                    struct_time = time.strptime(jobs['start_time'], "%Y-%m-%dT%H:%M:%S")
                    details['date'] = time.strftime("%Y-%m-%d %H:%M", struct_time)
                    for index,attr in enumerate(att_list):
                        if attr == 'avg_trans_rate_p_file':
                            data_string += (str(
                                float(jobs['finished'])/float(jobs['active'])*
                                float(jobs['bytes'])/float(jobs['active_time'])/
                                1000000.0
                            )+';')
                            att_string += 'Average transfer rate per file (MB/s);'
                        else:
                            data_string += (str(jobs[attr])+';')
                            att_string += '%s transfers;' % attr
                    details['plot_data'] = data_string[:-1]
                    list_of_details.append(details)
                break
        data['attribute'] = att_string[:-1]

        list_of_details = sorted(list_of_details, key = lambda k: k['date'])
        self.details_list = list_of_details
        data['tier_name'] = self.config['tier_name']
              
        ######################################
        ####PLOT#####################
        
        #Plot-lists
        self.plt = plt
        fig = self.plt.figure()
        
        default_attribute_indices = {'Average transfer rate per file (MB/s)':-1,'active transfers':-1,'finished trasfers':-1}
        available = {"rate":False,"active":False,"finished":False}
        datetime_list = []
        rate_list = []
        active_list = []
        finished_list = []
        attribute_list = data['attribute'].split(';')
        for attr,av in zip(default_attribute_indices,available):
            if attr in attribute_list: 
                default_attribute_indices[attr] = attribute_list.index(attr)
                available[av] = True
        for job in list_of_details:
            job_data = job['plot_data'].split(';')
            if available["rate"]: rate_list.append(float(job_data[default_attribute_indices['Average transfer rate per file (MB/s)']]))
            if available["active"]: active_list.append(float(job_data[default_attribute_indices['active transfers']]))
            if available["finished"]: finished_list.append(float(job_data[default_attribute_indices['finished transfers']]))
            
            datetime_list.append(job['date'])
        if available["rate"]:
            fig, ax1 = plt.subplots()
            index_list = np.arange(len(rate_list))
            ax1.set_title(self.config['tier_name'])
            ax1.set_xticks(index_list+0.5)
            ax1.set_xticklabels(datetime_list, rotation='vertical')
            ax2 = ax1.twinx()
            ax2.set_ylabel('Average transfer rate per file (MB/s)', color='r')
            
            
            transfers_list = []
            label = ""
            if available["active"]: transfers_list,label = active_list, "active transfers"
            if available["finished"]: transfers_list = finished_list, "finished transfers"
            if len(transfers_list) > 0:
                ax1.bar(index_list,transfers_list,1.0, color='darkslateblue')
                ax1.set_ylabel(label, color='darkslateblue')
                for tl in ax1.get_yticklabels():
                    tl.set_color('darkslateblue')
            smooth_index_list = np.linspace(index_list.min(),index_list.max(),300)
            smooth_rate_list = spline(index_list,rate_list,smooth_index_list)
            ax2.plot(smooth_index_list+0.5,smooth_rate_list,'r-')
            for tl in ax2.get_yticklabels():
                tl.set_color('r')
            

        else:
            pass
        #self.plt = plt
        #fig = self.plt.figure()
        #axis1 = fig.add_subplot(111)
        #ind = np.arange(len(data_list_1))
        #width = 1.0

        #max_value_1 = max([0] + data_list_1)
        #if max_value < 4.0:
        #    scale_value_1 = 0.2
        #else:
        #    scale_value_1 = max_value_1 // 4
        #
        #p1 = axis1.bar(ind, data_list_1, width, color='blue')
        #
        #axis1.set_position([0.2,0.3,0.75,0.6])
        #axis1.set_ylabel(data['attribute'])
        #axis1.set_title(self.config['tier_name'])
        #axis1.set_xticks(ind+width/2.)
        #axis1.set_xticklabels(datetime_list, rotation='vertical')
        #axis1.set_yticks(np.arange(0, max_value_1 + scale_value_1, scale_value_1))
        #
        #axis2 = axis1.twinx()
        #axis2.
        
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

