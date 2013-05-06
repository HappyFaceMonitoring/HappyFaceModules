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
from string import strip

class CMSPhedexAgents(hf.module.ModuleBase):
    
    config_keys = {
        'blacklist': ('list all agents to be neglected', ''),
        'eval_blacklist': ('list all agents, which should be shown but neglected in status evaluation', ''),
        'time_warning': ('time in seconds, if reports are older than this, agent status is switched to warning', '3600'),
        'time_critical': ('time in seconds, if reports are older than this, agent status is switched to warning', '7200'),
        'source_url': ('set url of source', 'both||url')
        }
    config_hint = ''
    
    table_columns = [
        Column('requestTime', FLOAT)
        ],[]
    
    subtable_columns = {
        'details': ([
            Column('name', TEXT),
            Column('host', TEXT),
            Column('label', TEXT),
            Column('version', TEXT),
            Column('state_dir', TEXT),
            Column('time_diff', FLOAT),
            Column('pid', INT),
            Column('status', TEXT)
            ],[])
        }
    
    def prepareAcquisition(self):
        self.time_warning = float(self.config['time_warning'])
        self.time_critical = float(self.config['time_critical'])
        self.source = hf.downloadService.addDownload(self.config['source_url'])
        self.details_db_value_list = []
        
        try:
            self.blacklist = map(strip, self.config['blacklist'].split(','))
        except AttributeError:
            self.blacklst = None
        try:
            self.eval_blacklist = map(strip, self.config['eval_blacklist'].split(','))
        except AttributeError:
            self.eval_blacklist = None
            
    def extractData(self):
        
        data = {}
        help_list = []
        data['source_url'] = self.source.getSourceUrl()
        root = etree.parse(open(self.source.getTmpPath())).getroot()
        status_list = []
        try:
            reqTime = data['requestTime'] = float(root.get('request_timestamp'))
        except KeyError:
            # raise apropriate Exception, currently not content in core :-/ 
            data['status'] = -1
            data['error_string'] = 'Request_Timestamp could not be found in downloaded file' 
            return data
        except ValueError:
            # raise apropriate Exception, currently not content in core :-/
            data['status'] = -1
            data['error_string'] = 'Request_Timestamp could not be converted to float, something is wrong with the file'
            return data
            
        for node in root:
            agent_dict = {}
            node_name = node.get('name')
            node_host = node.get('host')
            for agent in node:
                if self.blacklist is not None: 
                    if agent.get('label') not in self.blacklist:
                        agent_dict['label'] = agent.get('label')
                        agent_dict['version'] = agent.get('version')
                        agent_dict['state_dir'] = agent.get('state_dir')
                        agent_dict['time_diff'] = reqTime - float(agent.get('time_update'))
                        agent_dict['pid'] = int(agent.get('pid'))
                        agent_dict['name'] = node_name
                        agent_dict['host'] = node_host
                        if agent_dict['time_diff'] >= self.time_critical:
                            agent_dict['status'] = 'critical'
                        elif agent_dict['time_diff'] >= self.time_warning:
                            agent_dict['status'] = 'warning'
                        else:
                            agent_dict['status'] = 'ok'
                        help_list.append(agent_dict)
                else:
                    agent_dict['label'] = agent.get('label')
                    agent_dict['version'] = agent.get('version')
                    agent_dict['state_dir'] = agent.get('state_dir')
                    agent_dict['time_diff'] = reqTime - float(agent.get('time_update'))
                    agent_dict['pid'] = int(agent.get('pid'))
                    agent_dict['name'] = node_name
                    agent_dict['host'] = node_host
                    if agent_dict['time_diff'] >= self.time_critical:
                        agent_dict['status'] = 'critical'
                    elif agent_dict['time_diff'] >= self.time_warning:
                        agent_dict['status'] = 'warning'
                    else:
                        agent_dict['status'] = 'ok'
                    help_list.append(agent_dict)
            
            #there might be several entries for the same process, so you need to find the newest
        
        for i,agent in enumerate(help_list):
            pid = agent['pid']
            min_list = [agent]
            checked = True
            for j in range(i,len(help_list)):
                if pid == help_list[j]['pid']:
                    min_list.append(help_list[j])
            
            while True:
                if len(min_list)> 1:
                    if min_list[0]['time_diff'] < min_list[1]['time_diff']:
                        min_list.pop(1)
                    else:
                        min_list.pop(0)
                else:
                    break
            min_list = min_list[0]
            for p,q in enumerate(self.details_db_value_list):
                if min_list['pid'] == q['pid']:
                    checked = False
            if checked:
                self.details_db_value_list.append(min_list)
        
        if self.eval_blacklist is not None:
            for i,agent in enumerate(self.details_db_value_list):
                if agent['label'] not in self.eval_blacklist:
                    status_list.append(agent['status'])
        else:
            for i,agent in enumerate(self.details_db_value_list):
                status_list.append(agent['status'])
                
        if 'critical' in status_list:
            data['status'] = 0.0
        elif 'warning' in status_list:
            data['status'] = 0.5
        else:
            data['status'] = 1.0
        
        return data

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])
    
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).order_by(self.subtables['details'].c.name.asc()).execute().fetchall()
        details_list = [dict(time_str=self.formatTime(row['time_diff']), **row) for row in details_list]
        
        data['details'] = details_list
        return data
    
    def formatTime(self,time_diff):
        #TODO rewrite this function and use time package or something like that
        time_string = ""

        d = int(time_diff/24/3600)
        h = int((time_diff-d*24*3600)/3600)
        m = int((time_diff-d*24*3600-h*3600)/60)
        
        time_string = "%02id:%02ih:%02im" % (d, h, m)

        return time_string