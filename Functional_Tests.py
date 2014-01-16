# Copyright 2013 II. Institute of Physics - Georg-August University Goettingen
# Author: Christian Wehrberger (christian@wehrberger.de)
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
from sqlalchemy import *

class Functional_Tests(hf.module.ModuleBase):
    config_keys = {
        'url_aft': ('URL for analysis functional tests', 'both||hammercloud.cern.ch/hc/app/atlas/ssb/analysis/'),
        'url_pft': ('URL for production functional tests', 'both||hammercloud.cern.ch/hc/app/atlas/ssb/prod/'),
        'queues_aft': ('Queues to check for AFT', 'ANALY_GOEGRID'),
        'queues_aft_always_visible': ('Always visible AFT queues', 'ANALY_GOEGRID'),
        'queues_pft': ('Queues to check for PFT', 'GoeGrid'),
        'queues_pft_always_visible': ('Always visible PFT queues', 'GoeGrid'),
    }

    config_hint = ''

    table_columns = [
        Column('url_aft', TEXT),
        Column('url_pft', TEXT),
        Column('queues_aft', TEXT),
        Column('queues_pft', TEXT),
        Column('status_aft', FLOAT),
        Column('status_pft', FLOAT),
    ], []

    subtable_columns = {
        'aft_details': ([
        Column('queue_name', TEXT),
        Column('status', TEXT),
        Column('link', TEXT),
    ], []),
        'pft_details': ([
        Column('queue_name', TEXT),
        Column('status', TEXT),
        Column('link', TEXT),
    ], []),
    }

    def prepareAcquisition(self):
        queues_aft_string = self.config['queues_aft']
        queues_pft_string = self.config['queues_pft']

        # extract queues to list
        self.queues_aft = queues_aft_string.split(',')
        for index, queue in enumerate(self.queues_aft):
            self.queues_aft[index] = queue.strip()
        self.queues_aft[:] = [entry for entry in self.queues_aft if not entry == '']

        self.queues_pft = queues_pft_string.split(',')
        for index, queue in enumerate(self.queues_pft):
            self.queues_pft[index] = queue.strip()
        self.queues_pft[:] = [entry for entry in self.queues_pft if not entry == '']
        
        # prepare download (if key is of type local/global/both|options|url)
        self.source_aft = hf.downloadService.addDownload(self.config['url_aft'])
        self.source_pft = hf.downloadService.addDownload(self.config['url_pft'])

        self.subtable_aft_details = []
        self.subtable_pft_details = []

    def extractData(self):
        data = {
            'url_aft': self.config['url_aft'],
            'url_pft': self.config['url_pft'],
            'queues_aft': ','.join(self.queues_aft),
            'queues_pft': ','.join(self.queues_pft),
        }

        # assume everything is ok
        data['status'] = 1.
        
        # read content from downloaded files
        content_aft = open(self.source_aft.getTmpPath()).read()
        content_pft = open(self.source_pft.getTmpPath()).read()

        content_aft_list = content_aft.split('\n')
        content_pft_list = content_pft.split('\n')

        # parse information
        for index, line in enumerate(content_aft_list):
            line_list = line.split(' ')
            if len(line_list) != 6:
                continue
            for queue in self.queues_aft:
                if queue == line_list[2]:
                    self.subtable_aft_details.append({
                    'queue_name': queue,
                    'status': line_list[3],
                    'link': line_list[5],
                    })

        for index, line in enumerate(content_pft_list):
            line_list = line.split(' ')
            if len(line_list) != 6:
                continue
            for queue in self.queues_pft:
                if queue == line_list[2]:
                    self.subtable_pft_details.append({
                    'queue_name': queue,
                    'status': line_list[3],
                    'link': line_list[5],
                    })

        data['status_aft'] = 1
        # assess information
        for entry in self.subtable_aft_details:
            if not (entry['status'] == '100' or entry['status'] == 'no-test'):
                data['status'] = 0
                data['status_aft'] = 0

        data['status_pft'] = 1
        for entry in self.subtable_pft_details:
            if not (entry['status'] == '100' or entry['status'] == 'no-test'):
                data['status'] = 0
                data['status_pft'] = 0
        
        return data

    def fillSubtables(self, parent_id):
        self.subtables['aft_details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.subtable_aft_details])
        self.subtables['pft_details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.subtable_pft_details])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        aft_details = self.subtables['aft_details'].select().where(self.subtables['aft_details'].c.parent_id==self.dataset['id']).execute().fetchall()
        pft_details = self.subtables['pft_details'].select().where(self.subtables['pft_details'].c.parent_id==self.dataset['id']).execute().fetchall()

        data['aft_details'] = map(dict, aft_details)
        data['pft_details'] = map(dict, pft_details)

        queues_aft_always_visible_string = self.config['queues_aft_always_visible']
        queues_aft_always_visible = queues_aft_always_visible_string.split(',')
        for index, queue in enumerate(queues_aft_always_visible):
            queues_aft_always_visible[index] = queue.strip()
        queues_aft_always_visible[:] = [entry for entry in queues_aft_always_visible if not entry == '']
        data['queues_aft_always_visible'] = queues_aft_always_visible

        queues_pft_always_visible_string = self.config['queues_pft_always_visible']
        queues_pft_always_visible = queues_pft_always_visible_string.split(',')
        for index, queue in enumerate(queues_pft_always_visible):
            queues_pft_always_visible[index] = queue.strip()
        queues_pft_always_visible[:] = [entry for entry in queues_pft_always_visible if not entry == '']
        data['queues_pft_always_visible'] = queues_pft_always_visible

        return data



