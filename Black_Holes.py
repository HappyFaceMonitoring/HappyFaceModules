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
import re
from BeautifulSoup import BeautifulSoup

class Black_Holes(hf.module.ModuleBase):
    config_keys = {
        'source_url': ('Source URL', 'both||panda.cern.ch/server/pandamon/query?jobsummary=site&site=%s'),
        'queues': ('Name of the queues to check for black hole worker nodes', 'GoeGrid,ANALY_GOEGRID'),
        'black_hole_warning_threshold': ('If the number of failures of a specific worker node compared to the total number of failures exceeds this threshold, the status is set to warning.', '25'),
        'black_hole_critical_threshold': ('If the number of failures of a specific worker node compared to the total number of failures exceeds this threshold, the status is set to critical.', '50'),
    }

    table_columns = [
        Column('source_url', TEXT),
        Column('queues', TEXT),
    ], []

    subtable_columns = {
        'queue_details': ([
        Column('queue_name', TEXT),
        Column('worker_node', TEXT),
        Column('failed', INT),
    ], [])}

    def prepareAcquisition(self):
        queues_string = self.config['queues']
        self.queues = queues_string.split(',')
        for index, queue in enumerate(self.queues):
            self.queues[index] = queue.strip()
        self.queues[:] = [entry for entry in self.queues if not entry == '']

        # prepare download (if key is of type local/global/both|options|url)
        self.sources = {}
        for queue in self.queues:
            self.sources[queue] = hf.downloadService.addDownload(self.config['source_url']%(queue))

        self.subtable_queue_details = []

    def extractData(self):
        data = {
            'source_url': self.config['source_url'],
            'queues': ','.join(self.queues),
            'status': 1.,
        }

        self.failed = {}

        # read content from downloaded file
        contents = {}
        for queue in self.queues:
            contents[queue] = open(self.sources[queue].getTmpPath()).read()

        for queue in self.queues:
            table_start_identifier = re.compile('<table id=\'sitetable\'  class=\'display\' >(?!</table>)')
            table_end_identifier = re.compile('</table>')
            table_start = table_start_identifier.search(contents[queue]).span()[1]
            table_end = table_end_identifier.search((contents[queue])[table_start:]).span()[0]
            table_string = (contents[queue])[table_start:table_end+table_start]
            soup = BeautifulSoup(table_string)
            for body in soup.findAll('tbody'):
                for index, row in enumerate(body.findAll('tr')):
                    col = row.findAll('td')
                    if index == 0:
                        self.failed[queue] = int(col[6].string)
                    else:
                        worker_node = col[0].string
                        failed = int(col[6].string)
                        self.subtable_queue_details.append(
                            {
                                'queue_name': queue,
                                'worker_node': worker_node,
                                'failed': failed,
                            }
                        )

        for entry in self.subtable_queue_details:
            if self.failed[entry['queue_name']] == 0:
                data['status'] = min(data['status'],1)
            if float(entry['failed'])/float(self.failed[entry['queue_name']]) >= int(self.config['black_hole_critical_threshold']):
                data['status'] = min(data['status'],0)
            elif float(entry['failed'])/float(self.failed[entry['queue_name']]) >= int(self.config['black_hole_warning_threshold']):
                data['status'] = min(data['status'],0.5)
            else:
                data['status'] = min(data['status'],1)

        return data

    def fillSubtables(self, parent_id):
        self.subtables['queue_details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.subtable_queue_details])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        details = self.subtables['queue_details'].select().where(self.subtables['queue_details'].c.parent_id==self.dataset['id']).execute().fetchall()
        details_mapped = map(dict, details)

        data['details'] = sorted(details_mapped, key=lambda k: k['failed'], reverse=True)

        queues = self.dataset['queues'].split(',')
        failed = {}
        for queue in queues:
            failed[queue] = 0
            for detail in details:
                if detail['queue_name'] == queue:
                    failed[queue] += int(detail['failed'])
        data['failed'] = failed
        
        config_settings = {
            'black_hole_warning_threshold': int(self.config['black_hole_warning_threshold']),
            'black_hole_critical_threshold': int(self.config['black_hole_critical_threshold']),
        }
        data['config_settings'] = config_settings

        return data

