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
from BeautifulSoup import BeautifulSoup
import re

class Analysis_Ganga_Jobs(hf.module.ModuleBase):
    config_keys = {
        'source_url': ('Source URL for Ganga robot tests', 'both||panda.cern.ch:25980/server/pandamon/query?dash=analysis&processingType=gangarobot'),
        'queues': ('Queues to check', 'ANALY_GOEGRID'),
        'queues_always_visible': ('Queues that are always visible', 'ANALY_GOEGRID'),
        'failed_jobs_warning_threshold': ('Warning threshold for failed jobs in percent', '5'),
        'failed_jobs_critical_threshold': ('Critical threshold for failed jobs in percent', '10'),
    }

    config_hint = ''

    table_columns = [
        Column('source_url', TEXT),
        Column('queues', TEXT),
    ], []

    subtable_columns = {
        'queue_details': ([
        Column('queue_name', TEXT),
        Column('defined', INT),
        Column('assigned', INT),
        Column('waiting', INT),
        Column('activated', INT),
        Column('sent', INT),
        Column('running', INT),
        Column('holding', INT),
        Column('transferring', INT),  
        Column('finished', INT),
        Column('failed', INT),
    ], [])}

    def prepareAcquisition(self):
        queues_string = self.config['queues']
        self.queues = queues_string.split(',')
        for index, queue in enumerate(self.queues):
            self.queues[index] = queue.strip()
        self.queues[:] = [entry for entry in self.queues if not entry == '']

        # prepare download (if key is of type local/global/both|options|url)
        self.source = hf.downloadService.addDownload(self.config['source_url'])
        self.source_url = self.config['source_url'].split('|')[2]           

        self.subtable_queue_details = []

    def extractData(self):
        data = {
            'source_url': self.source_url,
            'queues': ','.join(self.queues),
            'status': 1.
        }

        # read content from downloaded file
        content = open(self.source.getTmpPath()).read()

        # fetch the table from the html input, identify the beginning of the table
        table_start_identifier = re.compile('<table border=0 cellpadding=3><tr align=center bgcolor=lightblue style\=\'font-weight: bold\'><td>Analysis Sites</td><td>Job<br>Nodes</td><td>Jobs</td><td>Latest</td><td>defined</td><td>assigned</td><td>waiting</td><td>activated</td><td>sent</td><td>running</td><td>holding</td><td>transferring</td><td>finished</td><td colspan=2>failed tot trf other</td></tr>(?!</table>)')
        table_end_identifier = re.compile('</table>')
        table_start = table_start_identifier.search(content).span()[1]
        table_end = table_end_identifier.search(content[table_start:]).span()[0]
        table_string = content[table_start:table_end+table_start]
        # parse html
        soup = BeautifulSoup(table_string)
        for row in soup.findAll('tr'):
            col = row.findAll('td')
            # extract relevant information
            queue_name = col[0].string
            defined = float(col[4].findAll('a')[0].string)
            assigned = float(col[5].findAll('a')[0].string)
            waiting = float(col[6].findAll('a')[0].string)
            activated = float(col[7].findAll('a')[0].string)
            sent = float(col[8].findAll('a')[0].string)
            running = float(col[9].findAll('a')[0].string)
            holding = float(col[10].findAll('a')[0].string)
            transferring = float(col[11].findAll('a')[0].string)
            finished = float(col[12].findAll('a')[0].string)
            failed = float(re.search(r'\d+', unicode(col[13])).group())
            
            # apply rating according to thresholds set in the module configuration, consider chosen queues only
            if queue_name in self.queues:
                if (failed + finished) == 0:
                    data['status'] = min(data['status'],1)
                elif failed/(failed + finished)*100. >= int(self.config['failed_jobs_critical_threshold']):
                    data['status'] = min(data['status'],0)
                elif failed/(failed + finished)*100. >= int(self.config['failed_jobs_warning_threshold']):
                    data['status'] = min(data['status'],0.5)
                else:
                    data['status'] = min(data['status'],1)

                self.subtable_queue_details.append(
                    {
                        'queue_name': queue_name,
                        'defined': defined,
                        'assigned': assigned,
                        'waiting': waiting,
                        'activated': activated,
                        'sent': sent,
                        'running': running,
                        'holding': holding,
                        'transferring': transferring,
                        'finished': int(finished),
                        'failed': int(failed),
                    }
                )

        return data

    def fillSubtables(self, parent_id):
        self.subtables['queue_details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.subtable_queue_details])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        details = self.subtables['queue_details'].select().where(self.subtables['queue_details'].c.parent_id==self.dataset['id']).execute().fetchall()
        # make config available in html template
        config_settings = {
            'failed_jobs_warning_threshold': int(self.config['failed_jobs_warning_threshold']),
            'failed_jobs_critical_threshold': int(self.config['failed_jobs_critical_threshold']),
        }

        data['details'] = map(dict, details)
        data['config_settings'] = config_settings

        # make always visible queues available in html template
        queues_always_visible_string = self.config['queues_always_visible']
        queues_always_visible = queues_always_visible_string.split(',')
        for index, queue in enumerate(queues_always_visible):
            queues_always_visible[index] = queue.strip()
        queues_always_visible[:] = [entry for entry in queues_always_visible if not entry == '']
        data['queues_always_visible'] = queues_always_visible

        return data

