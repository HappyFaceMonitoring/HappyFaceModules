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

class JobsStatistics(hf.module.ModuleBase):
    config_keys = {
        'min_jobs': ('Minimum number of jobs required for determining the module status', '100'),
        'warning_limit': ('Module turns yellow when a fraction q of the modules are ratio10', '0.3'),
        'critical_limit': ('Module turns red when a fraction q of the modules are ratio10', '0.5'),
        'old_result_warning_limit': ('Module turns yellow when input file is older than n hours', '1'),
        'old_result_critical_limit': ('Module turns red when input file is older than n hours', '4'),
        'groups': ('Colon separated user groups to include in the output, leave empty for all', ''),
        'rating_groups': ('Colon separated groups which will determine the output status', ''),
        'qstat_xml': ('URL of the input qstat xml file', ''),
    }
    config_hint = ''
    
    table_columns = [
        Column('details_group', TEXT),
        Column('result_timestamp', INT),
    ], []

    subtable_columns = {
        "groups": ([
            Column('group', TEXT, index = True),
            Column('parentgroup', TEXT),
            Column('total', INT),
            Column('running', INT),
            Column('ncpus', INT),
            Column('waiting', INT),
            Column('pending', INT),
            Column('ratio10', INT),
            Column('status', FLOAT),
        ], []),

        "details": ([
            Column('user', TEXT),
            Column('total', INT),
            Column('running', INT),
            Column('ncpus', INT),
            Column('pending', INT),
            Column('waiting', INT),
            Column('ratio100', INT),
            Column('ratio80', INT),
            Column('ratio30', INT),
            Column('ratio10', INT),
            Column('status', FLOAT),
        ], []),
    }
    
    
    def prepareAcquisition(self):
        # read configuration
        try:
            self.min_jobs = int(self.config["min_jobs"])
            self.warning_limit = float(self.config["warning_limit"])
            self.critical_limit = float(self.config["critical_limit"])

            self.old_result_warning_limit = float(self.config["old_result_warning_limit"])
            self.old_result_critical_limit = float(self.config["old_result_critical_limit"])

            self.groups = self.config["groups"].split(',')
            self.rating_groups = self.config["rating_groups"].split(',')
        except KeyError, e:
            raise hf.exceptions.ConfigError('Required parameter "%s" not specified' % str(e))

        if len(self.groups) == 0 or self.groups[0] == '':
            self.groups = None
        if len(self.rating_groups) == 0 or self.rating_groups[0] == '':
            self.rating_groups = None

        if 'qstat_xml' not in self.config: raise hf.exceptions.ConfigError('qstat_xml option not set')
        self.qstat_xml = hf.downloadService.addDownload(self.config['qstat_xml'])

        self.groups_db_value_list = []
        self.details_db_value_list = []


    def extractData(self):
        data = {'result_timestamp': 0, 'details_group': ''}
        data['source_url'] = self.qstat_xml.getSourceUrl()
        
        source_tree = etree.parse(open(self.qstat_xml.getTmpPath()))
        root = source_tree.getroot()

        # Check input file timestamp
        date = 0
        for element in root:
            if element.tag == "header":
                for child in element:
                    if child.tag == "date" and child.text is not None:
                        date = int(float(child.text.strip()))
        self.logger.debug('Date in header %i' % date)
        data['result_timestamp'] = date
        data['status'] = 1.0
        if self.run['time'] > datetime.datetime.fromtimestamp(date + self.old_result_critical_limit*3600):
            data['status'] = 0.0
        elif self.run['time']  >  datetime.datetime.fromtimestamp(date + self.old_result_warning_limit*3600):
            data['status'] = 0.5

        for element in root:
            if element.tag == "summaries":
                for child in element:
                    if child.tag == "summary":
                        group = 'all'
                        parent = ''
                        if 'group' in child.attrib:
                            group = child.attrib['group']
                        if 'parent' in child.attrib:
                            parent = child.attrib['parent']

                        if self.groups is not None and group not in self.groups:
                            continue

                        total = 0
                        ncpus = 0
                        running = 0
                        pending = 0
                        waiting = 0
                        ratio10 = 0
                        for subchild in child:
                            try:
                                if subchild.tag == 'jobs' and subchild.text is not None:
                                    total = int(subchild.text.strip())
                                if subchild.tag == 'ncpus' and subchild.text is not None:
                                    ncpus = int(subchild.text.strip())
                                if subchild.tag == 'running' and subchild.text is not None:
                                    running = int(subchild.text.strip())
                                if subchild.tag == 'pending' and subchild.text is not None:
                                    pending = int(subchild.text.strip())
                                if subchild.tag == 'waiting' and subchild.text is not None:
                                    waiting = int(subchild.text.strip())
                                if subchild.tag == 'ratio10' and subchild.text is not None:
                                    ratio10 = int(subchild.text.strip())
                            except ValueError:
                                # in case int() conversion fails
                                pass

                        status = 1.0
                        if running >= self.min_jobs and ratio10 >= running*self.warning_limit:
                            status = 0.5
                        if running >= self.min_jobs and ratio10 >= running*self.critical_limit:
                            status = 0.0

                        if self.rating_groups is None or group in self.rating_groups:
                            if status < data['status']:
                                data['status'] = status

                        groups_db_values = {}
                        groups_db_values["group"] = group
                        groups_db_values["parentgroup"] = parent
                        groups_db_values["total"] = total
                        groups_db_values["running"] = running
                        groups_db_values["ncpus"] = ncpus
                        groups_db_values["pending"] = pending
                        groups_db_values["waiting"] = waiting
                        groups_db_values["ratio10"] = ratio10
                        groups_db_values["status"] = status
                        self.groups_db_value_list.append(groups_db_values)

        users = {}
        for element in root:
            if element.tag == "jobs":
                group = ''
                if 'group' in element.attrib:
                    group = element.attrib['group']
                data["details_group"] = group

                for child in element:
                    if child.tag == "job":
                        user = ''
                        state = ''
                        cpueff = 0.0

                        for subchild in child:
                            try:
                                if subchild.tag == 'user' and subchild.text is not None:
                                    user = subchild.text.strip()
                                if subchild.tag == 'state' and subchild.text is not None:
                                    state = subchild.text.strip()
                                if subchild.tag == 'cpueff' and subchild.text is not None:
                                    cpueff = float(subchild.text.strip())
                                if subchild.tag == 'ncpus' and subchild.text is not None:
                                    ncpus = int(subchild.text.strip())
                            except ValueError:
                                # in case conversion fails
                                pass

                        if user == '' or state == '': continue
                        if user not in users:
                                users[user] = { 'total': 0, 'ncpus': 0, 'running': 0, 'pending': 0, 'waiting': 0, 'ratio100': 0, 'ratio80': 0, 'ratio30': 0, 'ratio10': 0 };

                        users[user]['total'] += 1
                        if state == 'running':
                        	users[user]['running'] += 1
                        	users[user]['ncpus'] += ncpus
                        elif state == 'pending': users[user]['pending'] += 1
                        elif state == 'waiting': users[user]['waiting'] += 1

                        if state == 'running':
                            if cpueff > 80: users[user]['ratio100'] += 1
                            elif cpueff > 30: users[user]['ratio80'] += 1
                            elif cpueff > 10: users[user]['ratio30'] += 1
                            else: users[user]['ratio10'] += 1

                # There should only be one jobs entry
                break

        # Do user rating
        for user in users:
            users[user]['user'] = user

            status = 1.0
            if users[user]['running'] >= self.min_jobs and users[user]['ratio10'] >= users[user]['running']*self.warning_limit:
                status = 0.5
            if users[user]['running'] >= self.min_jobs and users[user]['ratio10'] >= users[user]['running']*self.critical_limit:
                status = 0.0
            users[user]['status'] = status
        self.details_db_value_list = users.values()

        return data


    def fillSubtables(self, parent_id):
        self.subtables['groups'].insert().execute([dict(parent_id=parent_id, **row) for row in self.groups_db_value_list])
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])


    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        
        if data['run']['time'] > datetime.datetime.fromtimestamp(self.dataset['result_timestamp'] + float(self.config['old_result_critical_limit'])*3600):
            data['eval_time'] = True
        elif data['run']['time']  >  datetime.datetime.fromtimestamp(self.dataset['result_timestamp'] + float(self.config['old_result_warning_limit'])*3600):
            data['eval_time'] = True

        group_list = self.subtables['groups'].select().where(self.subtables['groups'].c.parent_id==self.dataset['id']).execute().fetchall()
        if group_list is None:
            group_list = []

        # convert RowProxy to dicts
        group_list = map(lambda x: dict(x), group_list)

        group_parents = dict((group['group'], group['parentgroup']) for group in group_list)
        group_children = {}
        for group in group_list:
            if group['parentgroup'] not in group_children:
                group_children[group['parentgroup']] = [group]
            else:
                group_children[group['parentgroup']].append(group)

        # calculate the level of indentation (num. of parents) for each group
        for idx,group in enumerate(group_list):
            num_parents = 0
            parent = group['parentgroup']
            while len(parent) > 0:
                num_parents += 1
                parent = group_parents[parent]
            group_list[idx]['indentation'] = num_parents
        self.logger.debug(group_list)

        # build the list again with the correct tree-like ordering
        group_tree_list = []
        def appendChildren(group):
            if group not in group_children:
                return
            for child in group_children[group]:
                group_tree_list.append(child)
                appendChildren(child['group'])
        if '' in group_children:
            appendChildren('')
        self.logger.debug(group_tree_list)
        data['group_list'] = group_tree_list

        # get the detailed information from database
        info_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['info_list'] = map(lambda x: dict(x), info_list)

        return data


