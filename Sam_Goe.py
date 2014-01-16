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
from datetime import datetime, timedelta
import json

class Sam_Goe(hf.module.ModuleBase):
    config_keys = {
        'sam_url': ('Source URL for json data', 'local||http://grid-monitoring.cern.ch/mywlcg/sam-pi/metrics_history_in_profile/?output=json'),
        'vo_names': ('Comma-separated list of VO names to check', 'atlas,ops'),
        'profile_names_vo_name': ('Comma-separated list of profile names to check for vo_name', ''),
        'service_hostnames': ('Comma-separated list of service hostnames to check', 'se-goegrid.gwdg.de'),
        'time_interval': ('Time interval to check in hours', '3'),
    }

    #config_hint = ''

    table_columns = [
        Column('sam_url', TEXT)
    ], []

    subtable_columns = {
        'test_details': ([
        Column('vo_name', TEXT),
        Column('profile_name', TEXT),
        Column('service_hostname', TEXT),
        Column('type', TEXT),
        Column('name', TEXT),
        Column('status', TEXT),
        Column('output_summary', TEXT),
        Column('execution_time', DATETIME)
    ], [])}

    def prepareAcquisition(self):
        sam_url = self.config['sam_url']
        full_sam_urls = {}
        self.sam_sources = {}
        vo_names = self.config['vo_names'].split(',')
        profile_names = {}
        for vo_name in vo_names:
            profile_names[vo_name] = self.config['profile_names'+'_'+vo_name].split(',')
        service_hostnames = self.config['service_hostnames'].split(',')
        end_time = datetime.utcnow()
        end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        start_time = end_time - timedelta(0,int(self.config['time_interval'])*3600)
        start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        for vo_name in vo_names:
            full_sam_urls[vo_name] = {}
            self.sam_sources[vo_name] = {}
            for profile_name in profile_names[vo_name]:
                full_sam_urls[vo_name][profile_name] = {}
                self.sam_sources[vo_name][profile_name] = {}
                for service_hostname in service_hostnames:
                    full_sam_urls[vo_name][profile_name][service_hostname] = sam_url + '&vo_name=' + vo_name + '&profile_name=' + profile_name + '&service_hostname=' + service_hostname + '&start_time=' + start_time_str + '&end_time=' + end_time_str
                    self.sam_sources[vo_name][profile_name][service_hostname] = hf.downloadService.addDownload(full_sam_urls[vo_name][profile_name][service_hostname])
                    #print(full_sam_urls[vo_name][profile_name][service_hostname])

        self.test_details_db_value_list = []

    def extractData(self):
        data = {
            'sam_url': self.config['sam_url'],
            'status': 1
        }

        for vo_name in self.sam_sources:
            for profile_name in self.sam_sources[vo_name]:
                for service_hostname in self.sam_sources[vo_name][profile_name]:
                    content = open(self.sam_sources[vo_name][profile_name][service_hostname].getTmpPath()).read()
                    json_content = json.loads(content)
                    type_list = []
                    if json_content:
                        if json_content[0]['services']:
                            for service in json_content[0]['services']:
                                name_list = []
                                if not service['type'] in type_list:
                                    type_list.append(service['type'])
                                    for entry in service['metrics']:
                                        if not entry['name'] in name_list:
                                            name_list.append(entry['name'])
                                            self.test_details_db_value_list.append({
                                                'vo_name': vo_name,
                                                'profile_name': profile_name,
                                                'service_hostname': service_hostname,
                                                'type': service['type'],
                                                'name': entry['name'],
                                                'status': entry['status'],
                                                'output_summary': entry['output_summary'],
                                                'execution_time':  datetime.strptime(entry['exec_time'], '%Y-%m-%dT%H:%M:%SZ')
                                            })
                                            if entry['status'].lower() == 'ok':
                                                data['status'] = min(data['status'],1)
                                            elif entry['status'].lower() == 'warning' or entry['status'].lower() == 'unknown':
                                                data['status'] = min(data['status'],0.5)
                                            elif entry['status'].lower() == 'critical':
                                                data['status'] = min(data['status'],0)

        return data

    def fillSubtables(self, parent_id):
        self.subtables['test_details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.test_details_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details = self.subtables['test_details'].select().where(self.subtables['test_details'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['details'] = map(dict, details)

        data['vo_names'] = self.config['vo_names'].split(',')
        profile_names = {}
        for vo_name in data['vo_names']:
            profile_names[vo_name] = self.config['profile_names'+'_'+vo_name].split(',')
        data['profile_names'] = profile_names
        data['service_hostnames'] = self.config['service_hostnames'].split(',')

        return data



