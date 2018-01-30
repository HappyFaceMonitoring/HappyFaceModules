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
import json
from sqlalchemy import TEXT, INT, Column


class CMSPhedexBlockReplicas(hf.module.ModuleBase):
    config_keys = {
        'source_url': ('set url of source', 'both|--no-check-certificate|https://cmsweb.cern.ch/phedex/datasvc'),
        'instance': 'prod',
        'node': 'T2_DE_DESY',
        'warning': ('Warning threshold [no. incomplete replicas]', '-1'),
        'critical': ('Critical threshold [no. incomplete replicas]', '-1'),
    }
    config_hint = ''

    table_columns = [Column('n_incomplete', INT)], []

    subtable_columns = {
        'details': ([Column('dataset', TEXT),
                     Column('block', TEXT),
                     Column('block_files', INT),
                     Column('resident_files', INT),
                     Column('block_size', INT),
                     Column('resident_size', INT),
                     Column('group', TEXT),
        ],[])
    }


    def prepareAcquisition(self):
        self.node = self.config['node']
        source_url = self.config['source_url']
        source_url += '/json/%s/blockreplicas?node=%s' % (self.config['instance'], self.node)
        self.source = hf.downloadService.addDownload(source_url)
        self.source_url = self.source.getSourceUrl()

        self.warning_threshold = int(self.config['warning'])
        self.critical_threshold = int(self.config['critical'])

        self.rows = []

    def extractData(self):
        with open(self.source.getTmpPath()) as in_f:
            data = json.load(in_f)

        n_incomplete = 0
        blocks = data['phedex']['block']
        for block in blocks:
            for replica in block['replica']:
                if replica['node'] != self.node:
                    continue  # should not happen with right node in config anyway
                if replica['complete'] != 'y':
                    n_incomplete += 1

                    ds_name, block_name = block['name'].split('#')
                    self.rows.append({
                        'dataset': ds_name,
                        'block': block_name,
                        'block_files': block['files'],
                        'resident_files': replica['files'],
                        'block_size': block['bytes'] / 1024**2,
                        'resident_size': replica['bytes'] / 1024**2,
                        'group': replica['group'],
                    })

        status = 1.
        if self.critical_threshold >= 0 and n_incomplete > self.critical_threshold:
            status = 0.
        elif self.warning_threshold >= 0 and n_incomplete > self.warning_threshold:
            status = .5

        return {'status': status, 'n_incomplete': n_incomplete}

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.rows])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id == self.dataset['id']).execute().fetchall()
        details_list = map(dict, details_list)

        data['details'] = details_list
        return data

