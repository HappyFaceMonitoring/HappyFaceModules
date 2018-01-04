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

import hf
from sqlalchemy import TEXT, INT, FLOAT, Column
from lxml import etree


class CMSPhedexPhysicsGroups(hf.module.ModuleBase):

    config_keys = {
        'source_url': ('set url of source', 'both||url'),
        'warning': ('Warning threshold [%]', '100'),
        'critical': ('Critical threshold [%]', '99'),
    }
    config_hint = ''

    table_columns = [],[]

    subtable_columns = {
        'details': ([Column('phys_group', TEXT),
                     Column('resident_data', FLOAT),
                     Column('subscribed_data', FLOAT),
                     Column('resident_files', INT),
                     Column('subscribed_files', INT),
                     Column('resident_data_perc', FLOAT),
                     Column('status', TEXT),
        ],[])
    }


    def prepareAcquisition(self):
        self.source = hf.downloadService.addDownload(self.config['source_url'])
        self.source_url = self.source.getSourceUrl()

        self.warning_threshold = float(self.config['warning']) / 100.
        self.critical_threshold = float(self.config['critical']) / 100.

        self.rows = []


    def extractData(self):
        status = 1.

        root = etree.parse(open(self.source.getTmpPath())).getroot()
        for node in root.findall('node'):
            for group in node.findall('group'):
                resident_data    = float(group.get('node_bytes')) / 1024**4
                subscribed_data  = float(group.get('dest_bytes')) / 1024**4
                resident_files   = int(group.get('node_files'))
                subscribed_files = int(group.get('dest_files'))

                group_status = 'ok'
                fraction = resident_data/subscribed_data
                if self.critical_threshold <= fraction < self.warning_threshold:
                    group_status = 'warning'
                    status = min(status, .5)
                elif fraction < self.critical_threshold:
                    group_status = 'critical'
                    status = 0.

                self.rows.append({
                    'phys_group': group.get('name'),
                    'resident_data': "%.1f" % (resident_data),
                    'subscribed_data': "%.1f" % (subscribed_data),
                    'resident_files': resident_files,
                    'subscribed_files': subscribed_files,
                    'resident_data_perc': "%.1f" % (fraction*100),
                    'status': group_status,
                })

        return {'status': status}


    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.rows])


    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id == self.dataset['id']).execute().fetchall()
        details_list = map(dict, details_list)

        data['details'] = details_list
        return data
