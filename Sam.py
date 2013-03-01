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


class Sam(hf.module.ModuleBase):
    config_keys = {
        'source_url': ('Source File', ''),
        'report_url': ('URL for detailed information in dashboard', 'http://dashb-cms-sum.cern.ch/dashboard/request.py/getMetricResultDetails'),
        'blacklist': ('Colon separated group to exclude from the output', ''),
        'storageelements': ('Colon separated list of storage element to include in the output', ''),
        'computingelements': ('Include all CEs with the given type', 'TYPE:CREAM-CE'),
        'computingelements_warning_numerror': ('', '>1'),
        'computingelements_error_numerror': ('', '>2'),
        'computingelements_warning_numwarning': ('', '>3'),
        'computingelements_warning_numtotal': ('', '<2'),
        'storageelements_error_numerror': ('', '>0'),
        'storageelements_error_numtotal': ('', '<1')
    }
    config_hint = "You're on your own with the thresholds..."
    
    table_columns = [], []

    subtable_columns = {
        'details': ([
            Column("service_type", TEXT),
            Column("service_name", TEXT),
            Column("service_status", FLOAT),
            Column("status", TEXT),
            Column("url", TEXT),
            Column("type", TEXT),
            Column("time", TEXT)
        ], []),

        'summary': ([
            Column("name", TEXT),
            Column("nodes", TEXT),
            Column("status", FLOAT)
        ], []),

        'individual': ([
            Column("name", TEXT),
            Column("status", TEXT),
            Column("type", TEXT)
        ], []),
    }
    
    
    def prepareAcquisition(self):                      
        try:
            self.report_url = self.config['report_url']
            if 'source_url' not in self.config: raise hf.exceptions.ConfigError('source_url option not set')
            self.source = hf.downloadService.addDownload(self.config['source_url'])
            self.blacklist = self.config['blacklist'].split(",")
        except KeyError, e:
            raise hf.exceptions.ConfigError('Required parameter "%s" not specified' % str(e))
            
        self.details_db_value_list = []
        self.details_summary_db_value_list = []
        self.individual_db_value_list = []
        self.SamResults = {}

    def extractData(self):
        # run the test
        data = {}
        data['source_url'] = self.source.getSourceUrl()
        
        with open(self.source.getTmpPath(), 'r') as f:
            data_object = json.loads(f.read())

        # parse the details and store it in a special database table
        
        try:
            for group in data_object[0]['groups']:
                for service in group['services']:
                    name = service['hostname']
                    if name in self.blacklist:
                        continue
                    self.SamResults[name] = {}
                    self.SamResults[name]["name"] = name
                    self.SamResults[name]["type"] = service['flavour']
                    query_base = 'hostName=' + name + '&flavour=' + self.SamResults[name]["type"]
                    service_status = 1.0
                    self.SamResults[name]["tests"] = []
                    for metric in service['metrics']:
                        details = {}
                        details["status"] = metric['status'].lower()
                        if (details["status"] == 'missing' or details["status"] == 'warning')and service_status > 0.5:
                            service_status = 0.5
                        elif details["status"] == 'critical' and service_status > 0.0:
                            service_status = 0.0
                        
                        details["url"] = self.report_url + '?' + query_base + '&metric=' + metric['name'] + '&timeStamp=' + metric['exec_time']
                        details["type"] = metric['name']
                        details["time"] = metric['exec_time']
                        self.SamResults[name]["tests"].append(details)
                    self.SamResults[name]["status"] = service_status
            
           
           
           
            samGroups = {}            
            groupConfig = {}
            groupConfig['computingelements'] = self.config['computingelements']
            groupConfig['storageelements'] = self.config['storageelements']
            
            for group in groupConfig.iterkeys():

                samGroups[group] = {}
                samGroups[group]['ident'] = groupConfig[group]
                samGroups[group]['nodes'] =[]
                samGroups[group]['numwarning'] = 0
                samGroups[group]['numerror'] = 0
                samGroups[group]['numok'] = 0
                samGroups[group]['numtotal'] = 0
                samGroups[group]['status'] = -1
            
                if samGroups[group]['ident'].find('Type:') == 0:
                    for type in samGroups[group]['ident'].split(','):
                        nodeclass = type.replace('Type:','')
                        for service in  self.SamResults.keys():
                            if  self.SamResults[service]['type'] == nodeclass:
                                samGroups[group]['nodes'].append(service)
                else:
                    samGroups[group]['nodes'] = samGroups[group]['ident'].split(',')
                samGroups[group]['nodes'].sort()

            groupThresholds = {}
            groupThresholds['computingelements_warning_numerror'] = self.config['computingelements_warning_numerror']
            groupThresholds['computingelements_error_numerror'] = self.config['computingelements_error_numerror']
            groupThresholds['computingelements_warning_numwarning'] = self.config['computingelements_warning_numwarning']
            groupThresholds['computingelements_warning_numtotal'] = self.config['computingelements_warning_numtotal']
            groupThresholds['storageelements_error_numerror'] = self.config['storageelements_error_numerror']
            groupThresholds['storageelements_error_numtotal'] = self.config['storageelements_error_numtotal']
            
            
            for val in groupThresholds.iterkeys():
                tmp = val.split('_')
                if len(tmp) != 3: self.error_message += "Config parameter "+val+" does not match group_Error/Warning."
                testCat = tmp[1]
                testValue = tmp[2]
                testRef = groupThresholds[val]

                if not samGroups.has_key(tmp[0]): next
                if not samGroups[ tmp[0] ].has_key(testCat): samGroups[ tmp[0] ][testCat] = []

                tmpThreshold = {}
                tmpThreshold['value'] = testValue
                tmpThreshold['condition'] =  str( testRef )[:1]
                tmpThreshold['threshold'] = float(str(testRef)[1:])
                samGroups[ tmp[0] ][testCat].append(tmpThreshold)





                     
            for group in samGroups:
                theGroup = samGroups[group]
                for service in theGroup['nodes']:
                    if self.SamResults[service]['status'] == 1.0:  theGroup['numok'] = theGroup['numok']+1
                    elif self.SamResults[service]['status'] == 0.5:  theGroup['numwarning'] =theGroup['numwarning']+1
                    elif self.SamResults[service]['status'] == 0.0: theGroup['numerror'] =theGroup['numerror']+1
                theGroup['numtotal'] = len( theGroup['nodes'] )
                

                if self.getGroupStatus(theGroup,'error') == True: theGroup['status'] = 0.0
                elif self.getGroupStatus(theGroup,'warning') == True: theGroup['status'] = 0.5
                else: theGroup['status'] = 1.0

        except Exception, ex:
            raise Exception('Could not extract any useful data from the XML source code for the status calculation:\n' + str(ex))
            

        theNodes = self.SamResults.keys()
        theNodes.sort()
        for service in theNodes:
            serviceInfo =  self.SamResults[service]

            details_db_values = {}
            details_db_values["service_type"] = serviceInfo['type'] 
            details_db_values["service_name"] = serviceInfo['name']
            details_db_values["service_status"] = serviceInfo['status']
            for test in  serviceInfo['tests']:
                for i in test.keys():
                    details_db_values[i] = test[i]
                self.details_db_value_list.append(details_db_values.copy())

        worstGroupStatus = 99.0
        
        if len(samGroups) > 0:
            for group,theGroup in samGroups.iteritems():
                details_summary_db_values = {}
                details_summary_db_values["name"] = group
                details_summary_db_values["nodes"] = ', '.join(theGroup['nodes'])
                details_summary_db_values["status"] = theGroup['status']
                self.details_summary_db_value_list.append(details_summary_db_values)
                if theGroup['status'] >= 0:
                    if theGroup['status'] < worstGroupStatus: worstGroupStatus = theGroup['status']
        else:
            for service in theNodes:
                 serviceInfo =  self.SamResults[service]
                 if serviceInfo['status'] >= 0:
                     if serviceInfo['status'] < worstGroupStatus: worstGroupStatus = serviceInfo['status']

        if worstGroupStatus != 99.0: self.status = worstGroupStatus
        else: self.status = -1
        
        for group, result in self.SamResults.iteritems():
            individual = {}
            individual['name'] = result['name']
            individual['type'] = result['type']
            if result['status'] == 1.0:
                individual['status'] = 'ok'
            elif result['status'] == 0.5:
                individual['status'] = 'warning'
            else:
                individual['status'] = 'critical'
            self.individual_db_value_list.append(individual)
        
        data['status'] = self.status
        return data
        
    def fillSubtables(self, parent_id):
        self.subtables['summary'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_summary_db_value_list])
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])
        self.subtables['individual'].insert().execute([dict(parent_id=parent_id, **row) for row in self.individual_db_value_list])
    
    def getTemplateData(self):
        
        data = hf.module.ModuleBase.getTemplateData(self)
        helpdata = {}
        
        details_list = self.subtables['details'].select()\
            .where(self.subtables['details'].c.parent_id==self.dataset['id'])\
            .order_by(self.subtables['details'].c.service_name.asc()).execute().fetchall()
        helpdata['details'] = map(dict, details_list)
        
        summary_list = self.subtables['summary'].select()\
            .where(self.subtables['summary'].c.parent_id==self.dataset['id'])\
            .order_by(self.subtables['summary'].c.name.asc()).execute().fetchall()
        helpdata['summary'] = map(dict, summary_list)
        
        individual_list = self.subtables['individual'].select()\
            .where(self.subtables['individual'].c.parent_id==self.dataset['id'])\
            .order_by(self.subtables['individual'].c.name.asc()).execute().fetchall()
        data['indsum'] = map(dict, individual_list)
        
        
        for i in range(len(helpdata['details'])):
            if helpdata['details'][i]['status'] == 'ok':
                helpdata['details'][i]['service_status'] = 1.0
            elif helpdata['details'][i]['status'] == 'missing' or helpdata['details'][i]['status'] == 'warning':
                helpdata['details'][i]['servive_status'] = 0.5
            else:
                helpdata['details'][i]['service_status'] = 0.0
                
            if helpdata['details'][i]['service_status'] == 1 or helpdata['details'][i]['service_status'] == '1.0':
                helpdata['details'][i]['service_status'] = 'ok'
            elif helpdata['details'][i]['service_status'] == 0.5 or helpdata['details'][i]['service_status'] == '0.5':
                helpdata['details'][i]['service_status'] = 'warning'
            else:
                helpdata['details'][i]['service_status'] = 'critical'
        
        for i in range(len(helpdata['summary'])):
            if helpdata['summary'][i]['status'] == 1.0 or helpdata['summary'][i]['status'] == '1.0':
               helpdata['summary'][i]['status'] = 'ok'
            elif helpdata['summary'][i]['status'] == 0.5 or helpdata['summary'][i]['status'] == '0.5':
                helpdata['summary'][i]['status'] = 'warning'
            else:
                helpdata['summary'][i]['status'] = 'critical'
                
        data['summary'] = helpdata['summary']
        data['critical_details'] = []
        data['ok_details'] = []
        temp = ''
        for current in range(len(helpdata['details'])):
            if helpdata['details'][current]['service_status'] == 'ok':
                data['ok_details'].append(helpdata['details'][current])
            else:
                data['critical_details'].append(helpdata['details'][current])
                
        return data
    
    def getGroupStatus(self,theGroup,type):
          if not theGroup.has_key(type): return False
          for check in theGroup[type]:
              if not theGroup.has_key(check['value']): next
              if check['condition'] == ">":
                  if theGroup[check['value']] > check['threshold']: return True
              if check['condition'] == "<":
                  if theGroup[check['value']] < check['threshold']: return True
          return False


    def printInfo(self):
        for service in self.SamResults.keys():

            serviceInfo =  self.SamResults[service]
            print serviceInfo['type']+" "+ serviceInfo['name'] +" "+str(serviceInfo['status'])
            for i in  serviceInfo['tests']:
                print i
            print '  '



    def determineStatus(self, serviceStatusList):
        status = 1.

        for serviceStatus in serviceStatusList:
            if serviceStatus < status:
                status = serviceStatus

        if len(serviceStatusList) == 0:
            status = -1.

        return status
    

    def determineTestStatus(self,StatusString):
        testStatus = 1.

        if StatusString == 'ok':
            testStatus = 1.
        elif StatusString == 'warn':
            testStatus = 0.5
        elif StatusString == 'error':
            testStatus = 0.

        return testStatus

    def determineServiceStatus(self,StatusList):
        status = 1.

        for testStatus in StatusList:
            if testStatus < status:
                status = testStatus

        if len(StatusList) == 0:
            status = -1.

        return status
        
