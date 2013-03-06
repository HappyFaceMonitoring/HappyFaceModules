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
from sqlalchemy import *
from lxml import etree

class CMSPhedexErrorLog(hf.module.ModuleBase):
    
    config_keys = {
        'link_direction': ("transfers 'from' or 'to' you", 'from'),
        'timerange_seconds': ('Ignore errors that are older than the specified time', '7200'),
        'source_url': ('use --no-check-certificate', ''),
        'min_error': ('minimal number of errors needed to determine status', '50'),
        'warning_dest': ('25% = 25', '25'),
        'critical_dest': ('25% = 25', '50'),
        'warning_source': ('25% = 25', '100'),
        'critical_source': ('25% = 25', '100'),
        'warning_trans': ('25% = 25', '100'),
        'critical_trans': ('25% = 25', '100'),
    }
    config_hint = 'If you have problems downloading your source file use: "source_url = both|--no-check-certificate|url"'
    
    table_columns = [
        Column('summary', FLOAT),
        Column('transfer', FLOAT),
        Column('destination', FLOAT),
        Column('source', FLOAT),
        Column('unknown', FLOAT),
        Column('frac_dest', FLOAT),
        Column('frac_source', FLOAT),
        Column('frac_trans', FLOAT),
        Column('frac_unknown', FLOAT),
        Column('transfer_status', TEXT),
        Column('destination_status', TEXT),
        Column('source_status', TEXT),
        Column('unknown_status', TEXT),
    ], []
    
    subtable_columns = {
        'details': ([
            Column('node', TEXT),
            Column('transfer', FLOAT),
            Column('destination', FLOAT),
            Column('source', FLOAT),
            Column('unknown', FLOAT),
            Column('trans_message', TEXT),
            Column('dest_message', TEXT),
            Column('source_message', TEXT),
            Column('unknown_message', TEXT)
        ], [])
    }

    
    def prepareAcquisition(self):
        self.link_direction = self.config['link_direction']
        if self.link_direction == 'from':
            self.link_direction = 'to'
        elif self.link_direction == 'to':
            self.link_direction = 'from'
        self.timerange_seconds = int(self.config['timerange_seconds'])
        self.min_error = float(self.config['min_error'])
        self.warning_dest = float(self.config['warning_dest'])
        self.critical_dest = float(self.config['critical_dest'])
        self.warning_source = float(self.config['warning_source'])
        self.critical_source = float(self.config['critical_source'])
        self.warning_trans = float(self.config['warning_trans'])
        self.critical_trans = float(self.config['critical_trans'])
        
        if 'source_url' not in self.config: raise hf.exceptions.ConfigError('source option not set')
        self.source = hf.downloadService.addDownload(self.config['source_url'])
        self.details_db_value_list = []
    
    def extractData(self):

        data = {}
        data['destination'] = 0
        data['source'] = 0
        data['transfer'] = 0
        data['unknown'] = 0
        data['summary'] = 0
        sourcedata = {}
        
        if self.source.isDownloaded():
            data['source_url'] = self.source.getSourceUrl()
            source_tree = etree.parse(open(self.source.getTmpPath()))
        else:
            self.source.error += '\t \t try option "--no-check-certificate" for parameter source_url'
            data['status'] = -1
            raise hf.exceptions.DownloadError(self.source)
        
        root = source_tree.getroot()
        request_time = float(root.get('request_timestamp'))
        for link in root:
            if link.tag == 'link':
                sourcedata[link.get(self.link_direction)] = []
                for block in link:
                    for file in block: 
                        if file.tag == 'file':
                            sourcedata[link.get(self.link_direction)].append(file)
        
        for site, files in sourcedata.iteritems():
            stash = {}
            stash['node'] = site
            stash['trans_message'] = 'Not set'
            stash['dest_message'] = 'Not set'
            stash['source_message'] = 'Not set'
            stash['unknown_message'] = 'Not set'
            transfer = 0
            source = 0
            destination = 0
            unknown = 0
            for file in files:
                atransfer = 0
                asource = 0
                adestination = 0
                aunknown = 0
                for errors in file:
                    if abs(request_time - float(errors.get('time_done'))) <= self.timerange_seconds:
                        for details in errors:
                            if details.tag == 'detail_log':
                                try:
                                    tempstring = details.text.split(' ')
                                    if tempstring[0] == 'TRANSFER' and atransfer == 0:
                                        transfer += 1
                                        atransfer = 1
                                        stash['trans_message'] = str(details.text)
                                    elif tempstring[0] == 'DESTINATION' and adestination == 0:
                                        destination += 1
                                        adestination = 1
                                        stash['dest_message'] = str(details.text)
                                    elif tempstring[0] == 'SOURCE' and asource == 0:
                                        source += 1
                                        asource = 1
                                        stash['source_message'] = str(details.text)
                                    elif aunknown == 0:
                                        unknown += 1
                                        aunknown = 1
                                        stash['unknown_message'] = str(details.text)
                                except AttributeError:
                                    unknown += 1
                                    aunknown = 1
                                    stash['unknown_message'] = 'Happyface was unable to parse this error, the logfile might be damaged'
            if transfer != 0 or source != 0 or destination !=0 or unknown != 0:
                stash['transfer'] = transfer
                stash['destination'] = destination
                stash['source'] = source
                stash['unknown'] = unknown
                data['destination'] += destination
                data['source'] += source
                data['transfer'] += transfer
                data['unknown'] += unknown
                self.details_db_value_list.append(stash)
        
        data['status'] = 1.0
        data['destination_status'] = 'ok'
        data['source_status'] = 'ok'
        data['transfer_status'] = 'ok'
        data['unknown_status'] = 'ok'
        data['summary'] = data['source'] + data['unknown'] + data['transfer'] + data['destination']
        
        try:
            data['frac_dest'] = float(data['destination'] * 100 / data['summary'])
            data['frac_source'] = float(data['source'] * 100 / data['summary'])
            data['frac_trans'] = float(data['transfer'] * 100 / data['summary'])
            data['frac_unknown'] = float(data['unknown'] * 100 / data['summary'])
        except ZeroDivisionError:
            data['frac_dest'] = 0
            data['frac_source'] = 0
            data['frac_trans'] = 0
            data['frac_unknown'] = 0
            
        if float(data['summary']) > self.min_error:
             
            if data['frac_dest'] >= self.warning_dest:
                data['destination_status'] = 'warning'
                data['status'] = 0.5
            elif data['frac_dest'] >= self.critical_dest:
                data['destination_status'] = 'critical'
                data['status'] = 0.0
            
            if data['frac_source'] >= self.warning_source:
                data['source_status'] = 'warning'
                data['status'] = 0.5
            elif data['frac_source'] >= self.critical_source:
                data['source_status'] = 'critical'
                data['status'] = 0.0
                
            if data['frac_trans'] >= self.warning_trans:
                data['transfer_status'] = 'warning'
                data['status'] = 0.5
            elif data['frac_trans'] >= self.critical_trans:
                data['transfer_status'] = 'critical'
                data['status'] = 0.0
            
            
        
        return data
        
    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])
        
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['details'] = map(dict, details_list)
        return data
