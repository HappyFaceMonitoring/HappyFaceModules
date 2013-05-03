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

import hf, logging
from sqlalchemy import *
from lxml.html import parse
from datetime import datetime
from datetime import timedelta

class dCacheDatasetRestoreLazy(hf.module.ModuleBase):
    config_keys = {
        'source': ('URL of the dCache Dataset Restore Monitor (Lazy)', ''),
        'stage_max_retry': ('Retry limit', '2'),
        'stage_max_time': ('Time limit (in hours)', '48'),
#       'details_cutoff': ('Max. number of details', '100'),
        'limit_warning': ('Warning limit', '5'),
        'limit_critical': ('Critical limit', '10')
    }
    config_hint = ''
    
    table_columns = [
        Column('total', INT),
        Column('total_problem', INT),
        Column('status_pool2pool', INT),
        Column('status_staging', INT),
        Column('status_waiting', INT),
        Column('status_suspended', INT),
        Column('status_unknown', INT),
        Column('time_limit', INT),
        Column('retry_limit', INT),
        Column('hit_retry', INT),
        Column('hit_time', INT),
    ], []

    subtable_columns = {'details': ([
        Column('pnfs', TEXT),
        Column('path', TEXT),
        Column('started_full', TEXT),
        Column('retries', INT),
        Column('status_short', TEXT),
    ], [])}
    

    def prepareAcquisition(self):
        
        self.source = hf.downloadService.addDownload(self.config['source'])
        self.stage_max_retry = int(self.config['stage_max_retry'])
        self.stage_max_time = int(self.config['stage_max_time'])
        self.limit_warning = int(self.config['limit_warning'])
        self.limit_critical = int(self.config['limit_critical'])
        
        try:
            self.details_cutoff = int(self.config['details_cutoff'])
        except hf.ConfigError:
            self.details_cutoff = 0

        self.statusTagsOK = ['Pool2Pool','Staging']
        self.statusTagsFail = ['Waiting','Suspended','Unknown']

        self.total = 0
        self.total_problem = 0
        self.hit_retry = 0
        self.hit_time = 0

        self.status = 1.0
       
        self.details_db_value_list = []

    def extractData(self):
        
        data = {'source_url': self.source.getSourceUrl(),
                'time_limit': self.stage_max_time,
                'retry_limit': self.stage_max_retry,
                'status': self.status}

        source_tree = parse(open(self.source.getTmpPath()))
        root = source_tree.getroot()

        stage_requests = []
        info = {}
        # parse html
        for td in root.findall('.//td'):
            tag = td.get('class')
            info[tag] = td.text
            if tag == 'path':
                stage_requests.append(info)
                info = {}

        self.total = len(stage_requests)
        data['total'] = self.total

        states = {}
        for tag in (self.statusTagsOK + self.statusTagsFail):
            states[tag] = 0
        
        count = 0
        for i in stage_requests:
            fail = False
            status = i['status'].split(' ')[0]
            if status in self.statusTagsFail:
                self.total_problem += 1
            states[status] += 1

            # Check if retry limit hit
            retries = int(i['retries'])
            if retries >= self.stage_max_retry:
                self.hit_retry += 1
                self.total_problem += 1
                fail = True
            
            # Check if time limit hit
            time_limit = timedelta(hours=self.stage_max_time)
            now = datetime.now()
            started = datetime.strptime(i['started'],'%m.%d %H:%M:%S')
            # No year information available, assume current year
            started = started.replace(year=now.year)
            # When timedelta is negative, assume last year
            if (now - started) < timedelta(0):
                started.replace(year=now.year-1)
            if (now - started) > time_limit:
                self.hit_time += 1
                self.total_problem += 1
                fail = True

            details_db_values = {}
            details_db_values['pnfs'] = i['pnfs']
            details_db_values['path'] = i['path']
            details_db_values['retries'] = i['retries']
            details_db_values['status_short'] = status
            details_db_values['started_full'] = i['started']
            if fail:
                count += 1
                if self.details_cutoff == 0 or count <= self.details_cutoff:
                    self.details_db_value_list.append(details_db_values)


        data['total_problem'] = self.total_problem
        data['hit_retry'] = self.hit_retry
        data['hit_time'] = self.hit_time
        
        for tag in (self.statusTagsOK + self.statusTagsFail):
            data['status_'+tag.lower()] = states[tag]

        if count >= self.limit_warning:
            self.status = 0.5
        if count >= self.limit_critical:
            self.status = 0.0
        data['status'] = self.status

        return data

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        info_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['info_list'] = map(dict, info_list)

        return data
