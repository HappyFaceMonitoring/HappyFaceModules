# -*- coding: utf-8 -*-
#
# Copyright 2013 Institut für Experimentelle Kernphysik - Karlsruher Institut für Technologie
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
import lxml.html
from lxml.html.clean import clean_html
import StringIO
from sqlalchemy import *

class FTSMonitor(hf.module.ModuleBase):
    
    config_keys = {
        'failed_transfers_threshold': (
                'maximum allowed percentage of failed jobs per channel', ''),
        'failed_channels_warning': (
                'number of channels above failed_transfers_threshold that \
                triggers a status warning', ''),
        'failed_channels_critical': (
                'number of channels above failed_transfers_threshold that \
                triggers a critical status', ''),
        'in_channel_filter_string': (
                'substring that needs to be contained in channel name to \
                mark incoming channel', ''),
        'out_channel_filter_string': (
                'substring that needs to be contained in channel name to \
                mark outgoing channel', ''),
        'source_url': (
                'URL to FTS Monitor', ''),
        'source_url_channel_members': (
                'URL to FTS Monitor Channel Member List', ''),
    }
    config_hint = ''
    
    table_columns = ([
        Column('total_in_channels', INT),
        Column('in_channels_above_failed_threshold', INT),
        Column('in_channels_above_failed_threshold_color', TEXT),
        Column('total_out_channels', INT),
        Column('out_channels_above_failed_threshold', INT),
        Column('out_channels_above_failed_threshold_color', TEXT),
        Column('failed_transfers_threshold', INT)], [])
    
    subtable_columns = {
        'in_channel_stats': ([
            Column('Channel', TEXT),
            Column('MembersFrom', TEXT),
            Column('MembersTo', TEXT),
            Column('Ready', INT),
            Column('Active', INT),
            Column('Finished', INT),
            Column('FinishedDirty', INT),
            Column('Failed', INT),
            Column('Canceled', INT)], []),
        'out_channel_stats': ([
            Column('Channel', TEXT),
            Column('MembersFrom', TEXT),
            Column('MembersTo', TEXT),
            Column('Ready', INT),
            Column('Active', INT),
            Column('Finished', INT),
            Column('FinishedDirty', INT),
            Column('Failed', INT),
            Column('Canceled', INT)], [])}
    
    def prepareAcquisition(self):
        try:
            self.failed_transfers_threshold = int(self.config['failed_transfers_threshold'])
            self.failed_channels_warning = int(self.config['failed_channels_warning'])
            self.failed_channels_critical = int(self.config['failed_channels_critical'])
            self.in_channel_filter_string = self.config['in_channel_filter_string']
            self.out_channel_filter_string = self.config['out_channel_filter_string']
        except KeyError, ex:
            raise hf.exceptions.ConfigError('Required parameter "%s" not specified' % str(e))
        url = []
        if 'source_url' not in self.config:
            raise hf.exceptions.ConfigError('No source URL specified')
        url.append(self.config['source_url'])
        if 'source_url_channel_members' not in self.config:
            raise hf.exceptions.ConfigError('No source URL for channel members specified')
        url.append(self.config['source_url_channel_members'])
        self.source = {}
        self.source_url = []
        for i, group in enumerate(url):
            self.source[i] = hf.downloadService.addDownload(group)
            self.source_url.append(self.source[i].getSourceUrl())
        self.in_details_db_value_list = []
        self.out_details_db_value_list = []
    
    def extractData(self):
        
        # access job statistics information
        data = {'latest_data_id': 0}
        webpage = open(self.source[0].getTmpPath())
        strwebpage = webpage.read()
        tree = lxml.html.parse(StringIO.StringIO(strwebpage))
        rowlist = tree.findall(".//tr")
        
        # parse job statistics for individual channels
        iBreachIn = 0
        iBreachOut = 0
        for irow in range(2,len(rowlist)):
            if len(rowlist[irow])==2: # rows of main table
                channel = str(rowlist[irow].get('id'))
                if self.in_channel_filter_string in channel:
                    in_channel_stats = {'Ready': int(0), 'MembersFrom': int(0),
                                        'MembersTo': int(0), 'Active': int(0),
                                        'Finished': int(0), 'FinishedDirty': int(0),
                                        'Failed': int(0), 'Canceled': int(0)}
                    in_channel_stats['Channel'] = channel
                    for i in range(0,len(rowlist[irow][1])):
                        strvalue = str(rowlist[irow][1][i].get('style')).replace(
                                'width: ','').replace('%','').replace('None','0')
                        in_channel_stats[str(rowlist[irow][1][i].get(
                                'class'))] += int(strvalue)
                    if in_channel_stats['Failed'] >= self.failed_transfers_threshold:
                        iBreachIn += 1
                    self.in_details_db_value_list.append(in_channel_stats)
                elif self.out_channel_filter_string in channel:
                    out_channel_stats = {'Ready': int(0), 'MembersFrom': int(0),
                                         'MembersTo': int(0), 'Active': int(0),
                                         'Finished': int(0), 'FinishedDirty': int(0),
                                         'Failed': int(0), 'Canceled': int(0)}
                    out_channel_stats['Channel'] = channel
                    for i in range(0,len(rowlist[irow][1])):
                        strvalue = str(rowlist[irow][1][i].get('style')).replace(
                                'width: ','').replace('%','').replace('None','0')
                        out_channel_stats[str(rowlist[irow][1][i].get(
                                'class'))] += int(strvalue)
                    if out_channel_stats['Failed'] >= self.failed_transfers_threshold:
                        iBreachOut += 1
                    self.out_details_db_value_list.append(out_channel_stats)

        # parse webpage containing information on channel members
        # leave the following line commented out, otherwise 'data source' in
        # 'show module information' won't be displayed correctly
        # data = {'source_url_channel_members': self.source_channel_members.getSourceUrl(),
        #         'latest_data_id': 0}
        webpage2 = open(self.source[1].getTmpPath())
        strwebpage2 = webpage2.read().replace("<br/>","\n")
        tree2 = lxml.html.parse(StringIO.StringIO(strwebpage2))
        rowlist2 = tree2.findall(".//tr")
        for irow in range(1,len(rowlist2)):
            MembersFrom = 0
            MembersTo = 0
            channel = str(rowlist2[irow].get('id'))
            if self.in_channel_filter_string in channel:
                # get member numbers on 'to' and 'from' side of channel
                try:
                    strMembersFrom = str(rowlist2[irow][1][1].text).split('\n')
                    MembersFrom = len(strMembersFrom)-1
                except:
                    MembersFrom = 1
                try:
                    strMembersTo = str(rowlist2[irow][2][1].text).split('\n')
                    MembersTo = len(strMembersTo)-1
                except:
                    MembersTo = 1
                # look up current channel and add member numbers on 'to' and 'from'
                # side of channel to in_channel_stats
                for i in range(0,len(self.in_details_db_value_list)):
                    if self.in_details_db_value_list[i]['Channel'] == channel:
                        self.in_details_db_value_list[i]['MembersFrom'] = MembersFrom
                        self.in_details_db_value_list[i]['MembersTo'] = MembersTo
                        break
            if self.out_channel_filter_string in channel:
                # get member numbers on 'to' and 'from' side of channel
                try:
                    strMembersFrom = str(rowlist2[irow][1][1].text).split('\n')
                    MembersFrom = len(strMembersFrom)-1
                except:
                    MembersFrom = 1
                try:
                    strMembersTo = str(rowlist2[irow][2][1].text).split('\n')
                    MembersTo = len(strMembersTo)-1
                except:
                    MembersTo = 1
                # look up current channel and add member numbers on 'to' and 'from'
                # side of channel to out_channel_stats
                for i in range(0,len(self.out_details_db_value_list)):
                    if self.out_details_db_value_list[i]['Channel'] == channel:
                        self.out_details_db_value_list[i]['MembersFrom'] = MembersFrom
                        self.out_details_db_value_list[i]['MembersTo'] = MembersTo
                        break
            pass
        
        data['total_in_channels'] = len(self.in_details_db_value_list)
        data['in_channels_above_failed_threshold'] = iBreachIn
        if iBreachIn >= self.failed_channels_critical:
            data['in_channels_above_failed_threshold_color'] = 'critical'
        elif iBreachIn >= self.failed_channels_warning:
            data['in_channels_above_failed_threshold_color'] = 'warning'
        else:
            # 'ok' colors cell green, '' leaves it blank
            data['in_channels_above_failed_threshold_color'] = ''
        data['total_out_channels'] = len(self.out_details_db_value_list)
        data['out_channels_above_failed_threshold'] = iBreachOut
        if iBreachOut >= self.failed_channels_critical:
            data['out_channels_above_failed_threshold_color'] = 'critical'
        elif iBreachOut >= self.failed_channels_warning:
            data['out_channels_above_failed_threshold_color'] = 'warning'
        else:
            # 'ok' colors cell green, '' leaves it blank
            data['out_channels_above_failed_threshold_color'] = ''
        data['failed_transfers_threshold'] = self.failed_transfers_threshold
        
        # set status
        if iBreachIn >= self.failed_channels_critical or \
                iBreachOut >= self.failed_channels_critical:
            data['status'] = 0.0
        elif iBreachIn >= self.failed_channels_warning or \
                iBreachOut >= self.failed_channels_warning:
            data['status'] = 0.5
        else:
            data['status'] = 1.0
        
        return data
    
    def fillSubtables(self, parent_id):
        self.subtables['in_channel_stats'].insert().execute(
                [dict(parent_id=parent_id, **row) for row in self.in_details_db_value_list])
        self.subtables['out_channel_stats'].insert().execute(
                [dict(parent_id=parent_id, **row) for row in self.out_details_db_value_list])
    
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        in_details_list = self.subtables['in_channel_stats'].select().where(
                self.subtables['in_channel_stats'].c.parent_id==self.dataset['id']
                ).execute().fetchall()
        data['in_channel_stats'] = map(dict, in_details_list)
        out_details_list = self.subtables['out_channel_stats'].select().where(
                self.subtables['out_channel_stats'].c.parent_id==self.dataset['id']
                ).execute().fetchall()
        data['out_channel_stats'] = map(dict, out_details_list)
        return data
