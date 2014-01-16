# Copyright 2013 II. Physikalisches Institut - Georg-August-Universitaet Goettingen
# Author: Christian Georg Wehrberger (christian@wehrberger.de)
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
import json

class DDM_Deletion(hf.module.ModuleBase):
    config_keys = {
        'site_name': ('Site name', 'GOEGRID'),
        'source_url': ('Source URL of the JSON file', 'both||http://bourricot.cern.ch/dq2/deletion/site/%s/period/1/format/json/'),
        'deletion_errors_warning_threshold': ('Warning threshold for deletion errors: If this threshold is reached, the status of the module will be warning', '80'),
        'deletion_errors_critical_threshold': ('Critical threshold for deletion errors: If this threshold is reached, the status of the module will be critical', '100'),
    }

    config_hint = ''

    table_columns = [
        Column('site_name', TEXT),
        Column('source_url', TEXT),
    ], []

    subtable_columns = {
        'space_tokens_details_table': ([
        Column('space_token_name', TEXT),
        Column('datasets_to_delete', INT),
        Column('datasets_waiting', INT),
        Column('datasets_resolved', INT),
        Column('datasets_queued', INT),
        Column('datasets_deleted', INT),
        Column('files_to_delete', INT),
        Column('files_deleted', INT),
        Column('gbs_to_delete', FLOAT),
        Column('gbs_deleted', FLOAT),
        Column('deletion_errors', INT),
        ], []),
    }

    def prepareAcquisition(self):
        self.site_name = self.config['site_name']
        self.source_url = self.config['source_url']%self.site_name

        # prepare downloads
        self.source = hf.downloadService.addDownload(self.source_url)

        self.space_tokens_details_table_db_value_list = []

    def extractData(self):
        data = {
            'site_name': self.site_name,
            'source_url': self.source_url.split('|')[2],
            'status': 1,
        }

        # read the downloaded files and parse json content
        json_string = open(self.source.getTmpPath()).read()
        json_content = json.loads(json_string)
        for entry in json_content:
            detail = {}
            detail['space_token_name'] = entry['pk']
            detail['datasets_to_delete'] = int(entry['fields']['datasets_to_delete'])
            detail['datasets_waiting'] = int(entry['fields']['waiting'])
            detail['datasets_resolved'] = int(entry['fields']['resolved'])
            detail['datasets_queued'] = int(entry['fields']['queued'])
            detail['datasets_deleted'] = int(entry['fields']['datasets'])
            detail['files_to_delete'] = int(entry['fields']['files_to_delete'])
            detail['files_deleted'] = int(entry['fields']['files'])
            detail['gbs_to_delete'] = float(entry['fields']['gbs_to_delete'])
            detail['gbs_deleted'] = float(entry['fields']['gbs'])
            detail['deletion_errors'] = int(entry['fields']['errors'])
            self.space_tokens_details_table_db_value_list.append(detail)

        # apply a rating
        for detail in self.space_tokens_details_table_db_value_list:
            if int(detail['deletion_errors']) >= int(self.config['deletion_errors_critical_threshold']):
                data['status'] = min(data['status'],0)
            elif int(detail['deletion_errors']) >= int(self.config['deletion_errors_warning_threshold']):
                data['status'] = min(data['status'],0.5)
            else:
                data['status'] = min(data['status'],1)

        return data

    def fillSubtables(self, parent_id):
        self.subtables['space_tokens_details_table'].insert().execute([dict(parent_id=parent_id, **row) for row in self.space_tokens_details_table_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        space_tokens_details = self.subtables['space_tokens_details_table'].select().where(self.subtables['space_tokens_details_table'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['details'] = map(dict, space_tokens_details)

        # make config settings available in html template
        data['config_settings'] = {}
        data['config_settings']['deletion_errors_warning_threshold'] = self.config['deletion_errors_warning_threshold']
        data['config_settings']['deletion_errors_critical_threshold'] = self.config['deletion_errors_critical_threshold']
        data['config_settings']['site_name'] = self.config['site_name']

        # generate summary data and make it available in html template
        data['summary'] = {
            'datasets_to_delete': 0,
            'datasets_waiting': 0,
            'datasets_resolved': 0,
            'datasets_queued': 0,
            'datasets_deleted': 0,
            'files_to_delete': 0,
            'files_deleted': 0,
            'gbs_to_delete': 0,
            'gbs_deleted': 0,
            'deletion_errors': 0,
        }
        for detail in data['details']:
            data['summary']['datasets_to_delete'] += int(detail['datasets_to_delete'])
            data['summary']['datasets_waiting'] += int(detail['datasets_waiting'])
            data['summary']['datasets_resolved'] += int(detail['datasets_resolved'])
            data['summary']['datasets_queued'] += int(detail['datasets_queued'])
            data['summary']['datasets_deleted'] += int(detail['datasets_deleted'])
            data['summary']['files_to_delete'] += int(detail['files_to_delete'])
            data['summary']['files_deleted'] += int(detail['files_deleted'])
            data['summary']['gbs_to_delete'] += float(detail['gbs_to_delete'])
            data['summary']['gbs_deleted'] += float(detail['gbs_deleted'])
            data['summary']['deletion_errors'] += int(detail['deletion_errors'])

        return data

