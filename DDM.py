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
from urllib2 import Request, urlopen, URLError
from decimal import *
import json

class DDM(hf.module.ModuleBase):
    config_keys = {
        'site_name': ('Site name', 'GOEGRID'),
        'cloud': ('Cloud', 'DE'),
        'time_interval': ('Time interval in minutes', '120'),
        'url_destination_space_tokens': ('URL for the destination space tokens', 'local||http://dashb-atlas-data.cern.ch/dashboard/request.py/matrix.json?activity=0&activity=1&activity=2&activity=3&activity=4&activity=5&activity=6&activity=7&src_grouping=cloud&dst_cloud=%%22%s%%22&dst_site=%%22%s%%22&dst_grouping=cloud&dst_grouping=site&dst_grouping=token&interval=%s'),
        'url_source_space_tokens': ('URL for the source space tokens', 'local||http://dashb-atlas-data.cern.ch/dashboard/request.py/matrix.json?activity=0&activity=1&activity=2&activity=3&activity=4&activity=5&activity=6&activity=7&src_cloud=%%22%s%%22&src_site=%%22%s%%22&src_grouping=cloud&src_grouping=site&src_grouping=token&dst_grouping=cloud&interval=%s'),
        'url_destination_failed_transfers': ('URL for failed transfers from destination', 'http://dashb-atlas-data.cern.ch/dashboard/request.py/details.json?activity=0&activity=1&activity=2&activity=3&activity=4&activity=5&activity=6&activity=7&dst_cloud=%%22%s%%22&dst_site=%%22%s%%22&state=FAILED_TRANSFER&error_code=&offset=0&limit=1000&from_date=%sT%s%%3A%s%%3A%s&to_date=%sT%s%%3A%s%%3A%s'),
        'url_source_failed_transfers': ('URL for failed transfers from source', 'http://dashb-atlas-data.cern.ch/dashboard/request.py/details.json?activity=0&activity=1&activity=2&activity=3&activity=4&activity=5&activity=6&activity=7&state=FAILED_TRANSFER&error_code=&offset=0&limit=1000&src_cloud=%%22%s%%22&src_site=%%22%s%%22&from_date=%sT%s%%3A%s%%3A%s&to_date=%sT%s%%3A%s%%3A%s'),
        'destination_warning_threshold': ('Below this efficiency for destination transfers, the status of the module will be warning (ok, if above).', '0.8'),
        'source_warning_threshold': ('Below this efficiency for destination transfers, the status of the module will be critical.', '0.8'),
        'destination_critical_threshold': ('Below this efficiency for source transfers, the status of the module will be warning (ok, if above).', '0.5'),
        'source_critical_threshold': ('Below this efficiency for source transfers, the status of the module will be critical.', '0.5'),
    }

    config_hint = 'Adjust the parameters site_name, cloud, and time_interval to your needs, as well as the thresholds for different statuses.'

    table_columns = [
        Column('site_name', TEXT),
        Column('cloud', TEXT),
        Column('time_interval', INT),
        Column('url_destination_space_tokens', TEXT),
        Column('url_source_space_tokens', TEXT),
        Column('url_source_space_tokens', TEXT),
        Column('url_destination_failed_transfers', TEXT),
        Column('url_source_failed_transfers', TEXT),
        Column('destination_successful_transfers_total', INT),
        Column('source_successful_transfers_total', INT), 
        Column('destination_failed_transfers_total', INT),
        Column('source_failed_transfers_total', INT),
        Column('destination_throughput_total', INT),
        Column('source_throughput_total', INT),
        Column('destination_failures_total', INT),
        Column('source_failures_total', INT),
        Column('destination_efficiency_total', FLOAT),
        Column('source_efficiency_total', FLOAT),
    ], []

    subtable_columns = {
        'destination_details_table': ([
        Column('token', TEXT),
        Column('successful', INT),
        Column('failed', INT),
        Column('failed_reason_destination', INT),
        Column('throughput', INT),
        Column('efficiency', FLOAT),
        ], []),

        'source_details_table': ([
        Column('token', TEXT),
        Column('successful', INT),
        Column('failed', INT),
        Column('failed_reason_source', INT),
        Column('throughput', INT),
        Column('efficiency', FLOAT),
        ], []),
    }

    def prepareAcquisition(self):
        self.source_url = 'www.google.com'
        self.site_name = self.config['site_name']
        self.cloud = self.config['cloud']
        self.time_interval = self.config['time_interval']
        self.url_destination_space_tokens = self.config['url_destination_space_tokens']%(str(self.cloud),str(self.site_name),self.time_interval)
        self.url_source_space_tokens = self.config['url_source_space_tokens']%(str(self.cloud),str(self.site_name),self.time_interval)
        self.url_destination_failed_transfers = self.config['url_destination_failed_transfers']
        self.url_source_failed_transfers = self.config['url_source_failed_transfers']

        # prepare downloads
        self.source_destination_space_tokens = hf.downloadService.addDownload(self.url_destination_space_tokens)
        self.source_source_space_tokens = hf.downloadService.addDownload(self.url_source_space_tokens)

        self.destination_details_table_db_value_list = []
        self.source_details_table_db_value_list = []

    def extractData(self):
        data = {
            'site_name': self.site_name,
            'cloud': self.cloud,
            'time_interval': int(self.time_interval),
            'url_destination_space_tokens': self.url_destination_space_tokens,
            'url_source_space_tokens': self.url_source_space_tokens,
        }

        # read the downloaded files
        content_destination_space_tokens = open(self.source_destination_space_tokens.getTmpPath()).read()
        content_source_space_tokens = open(self.source_source_space_tokens.getTmpPath()).read()

        # parse the source; due to the fact that some download links are created from other downloaded files, some downloads still have to take place here
        ddm_info = ddm_parser(self.cloud,self.site_name,content_destination_space_tokens,content_source_space_tokens,self.url_destination_failed_transfers,self.url_source_failed_transfers)

        data['destination_successful_transfers_total'] = ddm_info.destination_successful_transfers_total
        data['source_successful_transfers_total'] = ddm_info.source_successful_transfers_total
        data['destination_failed_transfers_total'] = ddm_info.destination_failed_transfers_total
        data['source_failed_transfers_total'] = ddm_info.source_failed_transfers_total
        data['destination_throughput_total'] = ddm_info.destination_throughput_total
        data['source_throughput_total'] = ddm_info.source_throughput_total
        data['destination_failures_total'] = ddm_info.destination_failures_total
        data['source_failures_total'] = ddm_info.source_failures_total
        if ddm_info.destination_successful_transfers_total + ddm_info.destination_failed_transfers_total != 0:
            data['destination_efficiency_total'] = ddm_info.destination_successful_transfers_total / (ddm_info.destination_successful_transfers_total + ddm_info.destination_failed_transfers_total)
        else:
            data['destination_efficiency_total'] = 0
        if ddm_info.source_successful_transfers_total + ddm_info.source_failed_transfers_total != 0:
            data['source_efficiency_total'] = ddm_info.source_successful_transfers_total / (ddm_info.source_successful_transfers_total + ddm_info.source_failed_transfers_total)
        else:
            data['source_efficiency_total'] = 0

        self.destination_details_table_db_value_list = [
            {
                'token': token,
                'successful': (ddm_info.destination_space_tokens[token])['successful'],
                'failed': (ddm_info.destination_space_tokens[token])['failed'],
                'failed_reason_destination': (ddm_info.destination_space_tokens[token])['failed_reason_destination'],
                'throughput': (ddm_info.destination_space_tokens[token])['throughput'],
                'efficiency': (ddm_info.destination_space_tokens[token])['efficiency'],
            }
            for token in ddm_info.destination_space_tokens
        ]
        self.source_details_table_db_value_list = [
            {
                'token': token,
                'successful': (ddm_info.source_space_tokens[token])['successful'],
                'failed': (ddm_info.source_space_tokens[token])['failed'],
                'failed_reason_source': (ddm_info.source_space_tokens[token])['failed_reason_source'],
                'throughput': (ddm_info.source_space_tokens[token])['throughput'],
                'efficiency': (ddm_info.source_space_tokens[token])['efficiency'],
            }
            for token in ddm_info.source_space_tokens
        ]

        data['status'] = 1
        for token in ddm_info.destination_space_tokens:
            if float(self.config['destination_warning_threshold']) <= (ddm_info.destination_space_tokens[token])['efficiency'] <= 1:
                data['status'] = min(data['status'], 1)
            elif float(self.config['destination_critical_threshold']) <= (ddm_info.destination_space_tokens[token])['efficiency'] < float(self.config['destination_warning_threshold']):
                data['status'] = min(data['status'], 0.5)
            elif 0 <= (ddm_info.destination_space_tokens[token])['efficiency'] < float(self.config['destination_critical_threshold']) and (ddm_info.destination_space_tokens[token])['successful'] + (ddm_info.destination_space_tokens[token])['failed'] > 0:
                data['status'] = min(data['status'], 0)
            else:
                data['status'] = min(data['status'], 0)
        for token in ddm_info.source_space_tokens:
            if float(self.config['source_warning_threshold']) <= (ddm_info.source_space_tokens[token])['efficiency'] <= 1:
                data['status'] = min(data['status'], 1)
            elif float(self.config['source_critical_threshold']) <= (ddm_info.source_space_tokens[token])['efficiency'] < float(self.config['source_warning_threshold']):
                data['status'] = min(data['status'], 0.5)
            elif 0 <= (ddm_info.source_space_tokens[token])['efficiency'] < float(self.config['source_critical_threshold']) and (ddm_info.source_space_tokens[token])['successful'] + (ddm_info.source_space_tokens[token])['failed'] > 0:
                data['status'] = min(data['status'], 0)
            else:
                data['status'] = min(data['status'], 0)

        return data

    def fillSubtables(self, parent_id):
        self.subtables['destination_details_table'].insert().execute([dict(parent_id=parent_id, **row) for row in self.destination_details_table_db_value_list])
        self.subtables['source_details_table'].insert().execute([dict(parent_id=parent_id, **row) for row in self.source_details_table_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        destination_details = self.subtables['destination_details_table'].select().where(self.subtables['destination_details_table'].c.parent_id==self.dataset['id']).execute().fetchall()
        source_details = self.subtables['source_details_table'].select().where(self.subtables['source_details_table'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['destination_warning_threshold'] = self.config['destination_warning_threshold']
        data['source_warning_threshold'] = self.config['source_warning_threshold']
        data['destination_critical_threshold'] = self.config['destination_critical_threshold']
        data['source_critical_threshold'] = self.config['source_critical_threshold']
        data['destination_details'] = map(dict, destination_details)
        data['source_details'] = map(dict, source_details)
        return data


class ddm_parser:
    def __init__(self, cloud, site_name, content_destination, content_source, link_destination_failed_transfers, link_source_failed_transfers):
        self.cloud = cloud
        self.site_name = site_name
        self.content_destination = content_destination
        self.content_source = content_source
        self.link_destination_failed_transfers = link_destination_failed_transfers
        self.link_source_failed_transfers = link_source_failed_transfers
        self.destination_space_tokens = {}
        self.source_space_tokens = {}
        self.destination_successful_transfers_total = 0
        self.source_successful_transfers_total = 0
        self.destination_failed_transfers_total = 0
        self.destination_throughput_total = 0
        self.source_throughput_total = 0
        self.source_failed_transfers_total = 0
        self.destination_failures_total = 0
        self.source_failures_total = 0
        self.parse_destination()
        self.parse_source()

    def __get_times(self,json_content):
        from_time = "n/a"
        from_time_dict = {}
        to_time = "n/a"
        to_time_dict = {}
        from_time = json_content['params']['from_date']
        from_time_dict['date'] = from_time.split("T")[0]
        from_time_dict['hh'] = from_time.split("T")[1].split(":")[0]
        from_time_dict['mm'] = from_time.split("T")[1].split(":")[1]
        from_time_dict['ss'] = from_time.split("T")[1].split(":")[2]
        to_time = json_content['params']['to_date']
        to_time_dict['date'] = to_time.split("T")[0]
        to_time_dict['hh'] = to_time.split("T")[1].split(":")[0]
        to_time_dict['mm'] = to_time.split("T")[1].split(":")[1]
        to_time_dict['ss'] = to_time.split("T")[1].split(":")[2]
        return from_time_dict, to_time_dict

    def parse_destination(self):
        content = self.content_destination
        json_content = json.loads(content)
        from_time, to_time = self.__get_times(json_content)
        if from_time == "n/a" or to_time == "n/a":
            print "No times defined, please check the __get_times() method"
            return "n/a"
        else:
            for transfer in json_content['transfers']['rows']:
                self.destination_successful_transfers_total += float(transfer[5])
                self.destination_failed_transfers_total += float(transfer[6])
                self.destination_throughput_total += float(transfer[4])
                if transfer[2] == self.site_name and transfer[3] in self.destination_space_tokens:
                    self.destination_space_tokens[transfer[3]] = {'successful': (self.destination_space_tokens[transfer[3]])['successful'] + float(transfer[5]), 'failed': (self.destination_space_tokens[transfer[3]])['failed'] + float(transfer[6]), 'failed_reason_destination': 0, 'throughput': (self.destination_space_tokens[transfer[3]])['throughput'] + float(transfer[4])}
                if  transfer[2] == self.site_name and transfer[3] not in self.destination_space_tokens:
                    self.destination_space_tokens[transfer[3]] = {'successful': float(transfer[5]), 'failed': float(transfer[6]), 'failed_reason_destination': 0, 'throughput': float(transfer[4])}
                
            destination_failure_info_link = self.link_destination_failed_transfers%(self.cloud, self.site_name, from_time['date'], from_time['hh'], from_time['mm'], from_time['ss'], to_time['date'], to_time['hh'], to_time['mm'], to_time['ss'])
            req = Request(destination_failure_info_link)
            try:
                response = urlopen(req)
            except URLError as e:
                if hasattr(e,'reason'):
                    print "Impossible to reach the server"
                    print "Reason: ", e.reason
                    return "n/a"
                elif hasattr(e, 'code'):
                    print "The server couldn't fulfill the request."
                    print "Error Code: ", e.code
                    return "n/a"
            else:
                destination_source = urlopen(destination_failure_info_link)
                destination_failures_json_content = json.load(destination_source)
                for transfer_detail in destination_failures_json_content['details']:
                    if "DESTINATION" in str(transfer_detail['transfer_error']):
                        self.destination_failures_total += 1
                    else:
                        continue

            for token in self.destination_space_tokens:
                if (self.destination_space_tokens[token])['successful'] + (self.destination_space_tokens[token])['failed'] != 0:
                    (self.destination_space_tokens[token])['efficiency'] = (self.destination_space_tokens[token])['successful'] / ((self.destination_space_tokens[token])['successful'] + (self.destination_space_tokens[token])['failed'])
                else:
                    (self.destination_space_tokens[token])['efficiency'] = 0
                destination_failure_info_link_token = self.link_destination_failed_transfers%(self.cloud, self.site_name, from_time['date'], from_time['hh'], from_time['mm'], from_time['ss'], to_time['date'], to_time['hh'], to_time['mm'], to_time['ss'])
                destination_failure_info_link_token += '&dst_token="'  + token + '"'
                req = Request(destination_failure_info_link)
                try:
                    response = urlopen(req)
                except URLError as e:
                    if hasattr(e,'reason'):
                        print "Impossible to reach the server"
                        print "Reason: ", e.reason
                        return "n/a"
                    elif hasattr(e, 'code'):
                        print "The server couldn't fulfill the request."
                        print "Error Code: ", e.code
                        return "n/a"
                else:
                    destination_source_token = urlopen(destination_failure_info_link_token)
                    destination_failure_json_content_token = json.load(destination_source_token)
                    for transfer_detail in destination_failure_json_content_token['details']:
                        if "DESTINATION" in str(transfer_detail['transfer_error']):
                            self.destination_space_tokens[token]['failed_reason_destination'] += 1
                        else:
                            continue



    def parse_source(self):
        content = self.content_source
        json_content = json.loads(content)
        from_time, to_time = self.__get_times(json_content)
        if from_time == "n/a" or to_time == "n/a":
            print "No times defined, please check the __get_times() method"
            return "n/a"
        else:
            for transfer in json_content['transfers']['rows']:
                self.source_successful_transfers_total += float(transfer[5])
                self.source_failed_transfers_total += float(transfer[6])
                self.source_throughput_total += float(transfer[4])
                if transfer[1] == self.site_name and transfer[2] in self.source_space_tokens:
                    self.source_space_tokens[transfer[2]] = {'successful': (self.source_space_tokens[transfer[2]])['successful'] + float(transfer[5]), 'failed': (self.source_space_tokens[transfer[2]])['failed'] + float(transfer[6]), 'failed_reason_source': 0, 'throughput': (self.source_space_tokens[transfer[2]])['throughput'] + float(transfer[4])}
                if  transfer[1] == self.site_name and transfer[2] not in self.source_space_tokens:
                    self.source_space_tokens[transfer[2]] = {'successful': float(transfer[5]), 'failed': float(transfer[6]), 'failed_reason_source': 0, 'throughput': float(transfer[4])}

            source_failure_info_link = self.link_source_failed_transfers%(self.cloud, self.site_name, from_time['date'], from_time['hh'], from_time['mm'], from_time['ss'], to_time['date'], to_time['hh'], to_time['mm'], to_time['ss'])
            req = Request(source_failure_info_link)
            try:
                response = urlopen(req)
            except URLError as e:
                if hasattr(e,'reason'):
                    print "Impossible to reach the server"
                    print "Reason: ", e.reason
                    return "n/a"
                elif hasattr(e, 'code'):
                    print "The server couldn't fulfill the request."
                    print "Error Code: ", e.code
                    return "n/a"
            else:
                source_source = urlopen(source_failure_info_link)
                source_failures_json_content = json.load(source_source)
                for transfer_detail in source_failures_json_content['details']:
                    if "SOURCE" in str(transfer_detail['transfer_error']):
                        self.source_failures_total += 1
                    else:
                        continue

            for token in self.source_space_tokens:
                if (self.source_space_tokens[token])['successful'] + (self.source_space_tokens[token])['failed'] != 0:
                    (self.source_space_tokens[token])['efficiency'] = (self.source_space_tokens[token])['successful'] / ((self.source_space_tokens[token])['successful'] + (self.source_space_tokens[token])['failed'])
                else:
                    (self.source_space_tokens[token])['efficiency'] = 0
                source_failure_info_link_token = self.link_source_failed_transfers%(self.cloud, self.site_name, from_time['date'], from_time['hh'], from_time['mm'], from_time['ss'], to_time['date'], to_time['hh'], to_time['mm'], to_time['ss'])
                source_failure_info_link_token += '&src_token="'  + token + '"'
                req = Request(source_failure_info_link)
                try:
                    response = urlopen(req)
                except URLError as e:
                    if hasattr(e,'reason'):
                        print "Impossible to reach the server"
                        print "Reason: ", e.reason
                        return "n/a"
                    elif hasattr(e, 'code'):
                        print "The server couldn't fulfill the request."
                        print "Error Code: ", e.code
                        return "n/a"
                else:
                    source_source_token = urlopen(source_failure_info_link_token)
                    source_failure_json_content_token = json.load(source_source_token)
                    for transfer_detail in source_failure_json_content_token['details']:
                        if "SOURCE" in str(transfer_detail['transfer_error']):
                            self.source_space_tokens[token]['failed_reason_source'] += 1
                        else:
                            continue
