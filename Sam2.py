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

import hf, lxml, logging, datetime
from sqlalchemy import *
from lxml import etree
import json
from string import strip


class Sam2(hf.module.ModuleBase):
    
    ##rebuild those config keys for the new module!
    config_keys = {
        'source_url': ('source url', 'both||url'),
        'report_url': ('URL for detailed information in dashboard', 'http://dashb-cms-sum.cern.ch/dashboard/request.py/getMetricResultDetails'),
        'blacklist': ('Colon separated group to exclude from the output, recommended list is for KIT', 'org.cms.glexec.WN-gLExec,org.cms.WN-swinst,org.cms.WN-xrootd-fallback,org.cms.WN-xrootd-access'),
        'service_flavour': ('colon seperated list of flavours to be extracted', 'CREAM-CE,SRMv2'),
        'service_type': ('colon seperated list of types to be extracted', 'CREAM-CE,SRMv2'),
        'SERVICE_warning_min_jobs': ('less tests will result in status warning for this service and the module', '3'),
        'SERVICE_error_min_jobs': ('less tests will result in status critical for this service and the module', '1'),
        'SERVICE_warning_warnings': ('this amount, or more warnings will result in status warning for module and service', '2'),
        'SERVICE_error_warnings': ('this amount, or more warnings will result in status critical for module and service', '4'),
        'SERVICE_warning_errors': ('this amount, or more errors will result in status warning for module and service', '1'),
        'SERVICE_error_errors': ('this amount, or more errors will result in status warning for module and service', '2')
    }
    config_hint = "Due to flexibility reasons you can configure crit and warn thresholds for each service, therefor replace SERVICE with service_type, use _ instead of - and all letters in lowercase"
    
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
            self.service_warning_min_jobs[stype] = int(self.config[str(service) + '_warning_min_jobs'])
            self.service_error_min_jobs[stype] = int(self.config[str(service) + '_error_min_jobs'])
            self.service_warning_warnings[stype] = int(self.config[str(service) + '_warning_warnings'])
            self.service_error_warnings[stype] = int(self.config[str(service) + '_error_warnings'])
            self.service_warning_errors[stype] = int(self.config[str(service) + '_warning_errors'])
            self.service_error_errors[stype] = int(self.config[str(service) + '_error_errors'])
        
        self.blacklist = map(strip, self.config['blacklist'].split(','))
        ## add download tyo queue
        self.source = hf.downloadService.addDownload(self.config['source_url'])
        ##initialize container for subtable data
        self.details_db_value_list = []

    def extractData(self):
        ##TODO currently no check if source is downloaded
        data = {}
        data['source_url'] = self.source.getSourceUrl()
        help_stati = []        
        ##Use json to extract the file
        with open(self.source.getTmpPath(), 'r') as f:
            services = json.loads(f.read())
        ##use first group available, you may change  this if you want to parse more than one site in one module
        services = services[0]['groups'][0]['services']
        
        ##parse your group and get all the tests
        for i,service in enumerate(services):
            if service['flavour'] in self.service_flavour and service['type'] in self.service_type:
                service_host = service['hostname']
                service_type = service['type']
                warnings = 0
                errors = 0
                tests = 0
                for j,test in enumerate(service['metrics']):
                    status_str = str(test['status']).lower()
                    if test['name'] not in self.blacklist:
                        if str(status_str) == 'warning':
                            warnings += 1
                        elif str(status_str) == 'critical':
                            errors += 1
                        tests += 1
                    self.details_db_value_list.append({'type':service_type, 'hostName':service_host, 'timeStamp':test['exec_time'], 'metric':test['name'], 'status':status_str})
                if tests < self.service_error_min_jobs[service_type] or errors >= self.service_error_errors[service_type] or warnings >= self.service_error_warnings[service_type]:
                    help_stati.append('critical')
                elif tests < self.service_warning_min_jobs[service_type] or errors >= self.service_warning_errors[service_type] or warnings >= self.service_warning_warnings[service_type]:
                    help_stati.append('warning')
                else:
                    help_stati.append('ok')           
        ##parsing the file is done, now evaluate everything
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
        self.service_warning_min_jobs = {}
        self.service_error_min_jobs = {}
        self.service_warning_warnings = {}
        self.service_error_warnings = {}
        self.service_warning_errors = {}
        self.service_error_errors = {}
        
        for stype in self.service_type:
            service = stype.replace('-','_').lower()
            self.service_warning_min_jobs[stype] = int(self.config[str(service) + '_warning_min_jobs'])
            self.service_error_min_jobs[stype] = int(self.config[str(service) + '_error_min_jobs'])
            self.service_warning_warnings[stype] = int(self.config[str(service) + '_warning_warnings'])
            self.service_error_warnings[stype] = int(self.config[str(service) + '_error_warnings'])
            self.service_warning_errors[stype] = int(self.config[str(service) + '_warning_errors'])
            self.service_error_errors[stype] = int(self.config[str(service) + '_error_errors'])
        
        data = hf.module.ModuleBase.getTemplateData(self)
        ok_test = []
        warning_test = []
        black_test = []
        hosts = {}
        host_ordered = []
        details_list = self.subtables['details'].select()\
            .where(self.subtables['details'].c.parent_id==self.dataset['id'])\
            .order_by(self.subtables['details'].c.hostName.asc()).execute().fetchall()
        ## sort data and seperate into blacklisted test, critical/warning tests and ok test and build a summary!
        
        for i,test in enumerate(map(dict, details_list)):
            if test['hostName'] not in hosts:
                hosts[test['hostName']]={'ok': 0, 'warn':0, 'status':'ok', 'sum':0, 'crit':0, 'type':test['type']}
                host_ordered.append({'name':test['hostName'], 'status':'ok', 'type':test['type']})
            if test['metric'] in self.blacklist:
                black_test.append(test)                
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
                
        for key,host in hosts.iteritems():
            service_type = host['type']
            if host['sum'] < self.service_error_min_jobs[service_type] or host['crit'] >= self.service_error_errors or host['warn'] >= self.service_error_warnings[service_type]:
                host['status'] = 'critical'
            elif host['sum'] < self.service_warning_min_jobs[service_type] or host['crit'] >= self.service_warning_errors[service_type] or host['warn'] >= self.service_warning_warnings[service_type]:
                host['status'] = 'warning'
            else:
                host['status'] = 'ok'
                
        for i,host in enumerate(host_ordered):
            host_ordered[i]['status'] = hosts[host['name']]['status']
        data['hosts'] = host_ordered
        data['url'] = self.base_url
        data['ok_test'] = ok_test
        data['warning_test'] = warning_test
        data['black_test'] = black_test
        
        return data