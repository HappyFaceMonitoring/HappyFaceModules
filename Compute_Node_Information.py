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

class Compute_Node_Information(hf.module.ModuleBase):
    config_keys = {
        'source_url': ('Source URL', 'both||panda.cern.ch/server/pandamon/query?jobsummary=site&site=%s'),
        'link_status': ('Link for displaying information on specific jobs statuses', 'http://panda.cern.ch/server/pandamon/query?job=*&site=%s&type=&hours=12&jobStatus=%s'),
        'queues': ('Name of the queues to check for black hole worker nodes', 'GoeGrid,ANALY_GOEGRID'),
        'failed_jobs_number_shown_as_warning': ('If there are any failed jobs, this parameter defines the number of compute nodes shown as warning', '3'),
        'failed_jobs_number_shown_as_critical': ('If there are any failed jobs, this parameter defines the number of compute nodes shown as critical', '2'),
        'transferring_jobs_number_shown': ('If there are any transferring jobs, this parameter defines a lower limit for the number of compute nodes shown (even if everything is ok)', '5'),
        'transferring_jobs_warning_threshold': ('If the number of transferring jobs is larger than or equal to this treshold, this is shown as warning', '5'),
        'transferring_jobs_critical_threshold': ('If the number of transferring jobs  is larger than or equal to this treshold, this is shown as critical', '10'),
    }

    table_columns = [
        Column('source_url', TEXT),
        Column('queues', TEXT),
    ], []

    subtable_columns = {
        'queue_details': ([
        Column('queue_name', TEXT),
        Column('worker_node', TEXT),
        Column('running', INT),
        Column('transferring', INT),
        Column('finished', INT),
        Column('failed', INT),
        Column('cancelled', INT),
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
                        running = int(col[2].string)
                        transferring = int(col[4].string)
                        finished = int(col[5].string)
                        failed = int(col[6].string)
                        cancelled = int(col[7].string)
                        self.subtable_queue_details.append(
                            {
                                'queue_name': queue,
                                'worker_node': worker_node,
                                'running': running,
                                'transferring': transferring,
                                'finished': finished,
                                'failed': failed,
                                'cancelled': cancelled,
                            }
                        )

        for entry in self.subtable_queue_details:
            if self.failed[entry['queue_name']] != 0:
                data['status'] = min(data['status'],0)
        for entry in self.subtable_queue_details:
            if entry['transferring'] >= int(self.config['transferring_jobs_warning_threshold']):
                data['status'] = min(data['status'],0.5)
            elif entry['transferring'] >= int(self.config['transferring_jobs_critical_threshold']):
                data['status'] = min(data['status'],1)

#        for entry in self.subtable_queue_details:
#            if self.failed[entry['queue_name']] == 0:
#                data['status'] = min(data['status'],1)
#            if float(entry['failed'])/float(self.failed[entry['queue_name']]) >= int(self.config['black_hole_critical_threshold']):
#                data['status'] = min(data['status'],0)
#            elif float(entry['failed'])/float(self.failed[entry['queue_name']]) >= int(self.config['black_hole_warning_threshold']):
#                data['status'] = min(data['status'],0.5)
#            else:
#                data['status'] = min(data['status'],1)

        return data

    def fillSubtables(self, parent_id):
        self.subtables['queue_details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.subtable_queue_details])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        details = self.subtables['queue_details'].select().where(self.subtables['queue_details'].c.parent_id==self.dataset['id']).execute().fetchall()
        details_mapped = map(dict, details)

        data['details'] = sorted(details_mapped, key=lambda k: k['failed'], reverse=True)
        data['details_transferring_sort'] = sorted(details_mapped, key=lambda k: k['transferring'], reverse=True)

        queues = self.dataset['queues'].split(',')
        failed = {}
        for queue in queues:
            failed[queue] = 0
            for detail in details:
                if detail['queue_name'] == queue:
                    failed[queue] += int(detail['failed'])
        data['failed'] = failed

        finished = {}
        for queue in queues:
            finished[queue] = 0
            for detail in details:
                if detail['queue_name'] == queue:
                    finished[queue] += int(detail['finished'])
        data['finished'] = finished

        transferring = {}
        for queue in queues:
            transferring[queue] = 0
            for detail in details:
                if detail['queue_name'] == queue:
                    transferring[queue] += int(detail['transferring'])
        data['transferring'] = transferring
        
        config_settings = {
            'link_status': self.config['link_status'],
            'failed_jobs_number_shown_as_warning': int(self.config['failed_jobs_number_shown_as_warning']),
            'failed_jobs_number_shown_as_critical': int(self.config['failed_jobs_number_shown_as_critical']),
            'transferring_jobs_number_shown': int(self.config['transferring_jobs_number_shown']),
            'transferring_jobs_warning_threshold': int(self.config['transferring_jobs_warning_threshold']),
            'transferring_jobs_critical_threshold': int(self.config['transferring_jobs_critical_threshold']),
        }
        data['config_settings'] = config_settings

        data['plotlist'] = {}
        for queue in queues:
            plot_list = []
            plot_list.append(['Compute Node', 'Failed Jobs (crit)', 'Failed Jobs (warn)', 'Failed Jobs (ok)'])
            iterator = 0
            for index, detail in enumerate(data['details']):
                if detail['queue_name'] == queue:
                    if int(detail['failed']) == 0:
                        break
                    if iterator < int(self.config['failed_jobs_number_shown_as_critical']):
                        plot_list.append([
                            str(detail['worker_node']),
                            detail['failed'],
                            0,
                            0
                        ])
                    elif iterator < int(self.config['failed_jobs_number_shown_as_critical']) + int(self.config['failed_jobs_number_shown_as_warning']):
                        plot_list.append([
                            str(detail['worker_node']),
                            0,
                            detail['failed'],
                            0
                        ])
                    else:
                        plot_list.append([
                            str(detail['worker_node']),
                            0,
                            0,
                            detail['failed']
                        ])
                    iterator += 1
            data['plotlist'][queue] = plot_list

        return data

