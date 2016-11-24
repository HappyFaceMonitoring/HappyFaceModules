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
from sqlalchemy import TEXT, Column
import json
from string import strip


class Sam2(hf.module.ModuleBase):

    shwsw_help = ("if amount of warnings is equal or "
        "greater than this threshold, "
        "service and happyface status "
        "will change to warning")
    shcsw_help = ("if amount of warnings is equal or "
        "greater than this threshold, "
        "service and happyface status "
        "will change to critical")
    shwse_help = ("if amount of errors is equal or "
        "greater than this threshold, "
        "service and happyface status "
        "will change to warning")
    shcse_help = ("if amount of errors is equal or "
        "greater than this threshold, "
        "service and happyface status "
        "will change to critical")

    ##rebuild those config keys for the new module!
    config_keys = {
        'source_url': ('source url', 'both||url'),
        'report_url': ('URL for detailed information in dashboard', \
            'http://dashb-cms-sum.cern.ch/dashboard/request.py/getMetricResultDetails'),
        'blacklist': ('Colon separated group to exclude from the output, recommended list is for KIT', \
            'org.cms.glexec.WN-gLExec,org.cms.WN-swinst,org.cms.WN-xrootd-fallback,org.cms.WN-xrootd-access'),
        'service_flavour': ('colon seperated list of flavours to be extracted', 'CREAM-CE,SRMv2'),
        'service_type': ('colon seperated list of types to be extracted', 'CREAM-CE,SRMv2'),
        'SERVICE_hf_warning_sam_min_jobs': ('less tests will result in status warning for this service and the module', '3'),
        'SERVICE_hf_critical_sam_min_jobs': ('less tests will result in status critical for this service and the module', '1'),
        'SERVICE_hf_warning_sam_warnings': (shwsw_help, '2'),
        'SERVICE_hf_critical_sam_warnings': (shcsw_help, '4'),
        'SERVICE_hf_warning_sam_errors': (shwse_help, '1'),
        'SERVICE_hf_critical_sam_errors': (shcse_help, '2'),
        'ce_blacklist': ('Colon separated group of CEs to exclude from the output', 'None')
    }
    config_hint = ("Due to flexibility reasons "
        "you can configure crit and warn thresholds for each service, "
        "therefore replace SERVICE with service_type, "
        "use _ instead of - and all letters in lowercase"
        )

    table_columns = [], []

    subtable_columns = {
        'details': ([
            Column("type", TEXT),
            Column("hostName", TEXT),
            Column("timeStamp", TEXT),
            Column("metric", TEXT),
            Column("status", TEXT)
        ], [])
    }


    def prepareAcquisition(self):
        ## get config information
        self.service_flavour = map(strip, str(self.config['service_flavour']).split(','))
        self.service_type = map(strip, str(self.config['service_type']).split(','))

        self.service_warning_min_jobs = {}
        self.service_error_min_jobs = {}
        self.service_warning_warnings = {}
        self.service_error_warnings = {}
        self.service_warning_errors = {}
        self.service_error_errors = {}

        for stype in self.service_type:
            service = stype.replace('-','_').lower()
            self.service_warning_min_jobs[stype] = int(self.config[str(service) + '_hf_warning_sam_min_jobs'])
            self.service_error_min_jobs[stype] = int(self.config[str(service) + '_hf_critical_sam_min_jobs'])
            self.service_warning_warnings[stype] = int(self.config[str(service) + '_hf_warning_sam_warnings'])
            self.service_error_warnings[stype] = int(self.config[str(service) + '_hf_critical_sam_warnings'])
            self.service_warning_errors[stype] = int(self.config[str(service) + '_hf_warning_sam_errors'])
            self.service_error_errors[stype] = int(self.config[str(service) + '_hf_critical_sam_errors'])

        self.blacklist = map(strip, self.config['blacklist'].split(','))
        self.ce_blacklist = map(strip, self.config['ce_blacklist'].split(','))
        ## add download tyo queue
        self.source = hf.downloadService.addDownload(self.config['source_url'])
        self.source_url = self.source.getSourceUrl()
        ##initialize container for subtable data
        self.details_db_value_list = []

    def extractData(self):
        ##TODO currently no check if source is downloaded
        data = {}
        help_stati = []
        ##Use json to extract the file
        with open(self.source.getTmpPath(), 'r') as f:
            services = json.loads(f.read())
        ##use first group available, you may change  this if you want to parse more than one site in one module
        services = services['data']['results'][0]['flavours']

        ##parse your group and get all the tests
        for service in services:
            if service['flavourname'] in self.service_flavour and service['servicename'] in self.service_type:
                for host in service['hosts']:
                    service_host = host['hostname']
                    host_status = host['hostStatus']
                    service_type = service['servicename']
                    warnings = 0
                    errors = 0
                    tests = 0
                    test = None
                    for test in host['metric']:
                        status_str = str(test['status']).lower()
                        if test['metric_name'] not in self.blacklist:
                            if str(status_str) == 'warning':
                                warnings += 1
                            elif str(status_str) == 'critical':
                                errors += 1
                            tests += 1
                        self.details_db_value_list.append({'type':service_type, \
                            'hostName':service_host, 'timeStamp':test['timestamp'], \
                            'metric':test['metric_name'], 'status':status_str} \
                            )
                    self.details_db_value_list.append({'type':service_type, \
                        'hostName':service_host, 'timeStamp': '', \
                        'metric':'summary_%s' % test['metric_name'], \
                        'status':host_status})
                    if service_host in self.ce_blacklist:
                        continue
                    if tests < self.service_error_min_jobs[service_type] or \
                        errors >= self.service_error_errors[service_type] or \
                        warnings >= self.service_error_warnings[service_type]:
                        help_stati.append('critical')
                    elif tests < self.service_warning_min_jobs[service_type] or \
                        errors >= self.service_warning_errors[service_type] or \
                        warnings >= self.service_warning_warnings[service_type]:
                        help_stati.append('warning')
                    else:
                        help_stati.append('ok')
        ##parsing of the file is done, now evaluate everything
        if 'critical' in help_stati:
            data['status'] = 0
        elif 'warning' in help_stati:
            data['status'] = 0.5
        else:
            data['status'] = 1
        return data

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])

    def getTemplateData(self):
        ## you need the configuration again, because you must evaluate your summary again!
        self.base_url = self.config['report_url']
        self.service_flavour = map(strip, str(self.config['service_flavour']).split(','))
        self.service_type = map(strip, str(self.config['service_type']).split(','))
        self.blacklist = map(strip, self.config['blacklist'].split(','))
        self.ce_blacklist = map(strip, self.config['ce_blacklist'].split(','))
        self.service_warning_min_jobs = {}
        self.service_error_min_jobs = {}
        self.service_warning_warnings = {}
        self.service_error_warnings = {}
        self.service_warning_errors = {}
        self.service_error_errors = {}

        for stype in self.service_type:
            service = stype.replace('-','_').lower()
            self.service_warning_min_jobs[stype] = int(self.config[str(service) + '_hf_warning_sam_min_jobs'])
            self.service_error_min_jobs[stype] = int(self.config[str(service) + '_hf_critical_sam_min_jobs'])
            self.service_warning_warnings[stype] = int(self.config[str(service) + '_hf_warning_sam_warnings'])
            self.service_error_warnings[stype] = int(self.config[str(service) + '_hf_critical_sam_warnings'])
            self.service_warning_errors[stype] = int(self.config[str(service) + '_hf_warning_sam_errors'])
            self.service_error_errors[stype] = int(self.config[str(service) + '_hf_critical_sam_errors'])

        data = hf.module.ModuleBase.getTemplateData(self)
        ok_test = []
        summary_list = []
        warning_test = []
        black_test = []
        hosts = {}
        host_ordered = []
        details_list = self.subtables['details'].select()\
            .where(self.subtables['details'].c.parent_id==self.dataset['id'])\
            .order_by(self.subtables['details'].c.hostName.asc()).execute().fetchall()
        ## sort data and seperate into blacklisted test, critical/warning tests and ok test and build a summary!

        for test in map(dict, details_list):
            test['metricfqan'] = test['metric'].replace(' ', '%20').replace('/', '_')
            if test['hostName'] not in hosts and str(test['metric'][0:7]) != 'summary':
                hosts[test['hostName']]={'ok': 0, 'warn':0, 'status':'ok', 'sum':0, 'crit':0, 'type':test['type']}
            if test['metric'] in self.blacklist or test['hostName'] in self.ce_blacklist:
                black_test.append(test)
            elif str(test['metric'][0:7]) == 'summary':
                summary_list.append(test)
                host_ordered.append({'name':test['hostName'], 'status': test['status'].lower(), 'hostStatus': test['status'], 'type':test['type']})
            elif str(test['status']) == 'warning':
                warning_test.append(test)
                hosts[test['hostName']]['warn'] += 1
                hosts[test['hostName']]['sum'] += 1
            elif str(test['status']) == 'critical':
                warning_test.append(test)
                hosts[test['hostName']]['crit'] += 1
                hosts[test['hostName']]['sum'] += 1
            elif str(test['status']) == 'ok':
                ok_test.append(test)
                hosts[test['hostName']]['ok'] += 1
                hosts[test['hostName']]['sum'] += 1

        for host in hosts.itervalues():
            service_type = host['type']
            if host['sum'] < self.service_error_min_jobs[service_type] or \
                host['crit'] >= self.service_error_errors[service_type] or \
                host['warn'] >= self.service_error_warnings[service_type]:
                host['status'] = 'critical'
            elif host['sum'] < self.service_warning_min_jobs[service_type] or \
                host['crit'] >= self.service_warning_errors[service_type] or \
                host['warn'] >= self.service_warning_warnings[service_type]:
                host['status'] = 'warning'
            else:
                host['status'] = 'ok'

        data['hosts'] = host_ordered
        data['url'] = self.base_url
        data['ok_test'] = ok_test
        data['warning_test'] = warning_test
        data['black_test'] = black_test
        data['summary_list'] = summary_list

        return data
