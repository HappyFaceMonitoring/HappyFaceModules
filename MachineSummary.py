# -*- coding: utf-8 -*-
#
# Copyright 2015 Institut für Experimentelle Kernphysik - Karlsruher Institut für Technologie
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
from sqlalchemy import TEXT, Column, INT
import json

class MachineSummary(hf.module.ModuleBase):
	config_keys={'source_url': ('',''),
		     'file_location': ('The location of the json file.','')
	}
	
	table_columns = [],[]
	subtable_columns = {'statistics': ([
			    	Column('ip', TEXT),
				Column('state', TEXT),
				Column('hostname', TEXT),
				Column('wiki_link', TEXT),
				Column('ssh_hostname', TEXT),
				Column('last_user', TEXT),
				Column('os', TEXT),
				Column('vm_guest', TEXT),
				Column('vm_host', TEXT),
				Column('ansible_roles', TEXT),
				Column('notes', TEXT),
				Column('nagios_link', TEXT),
				Column('nagios_warn', INT),
				Column('nagios_crit', INT)
			],[])
	}
	
	def prepareAcquisition(self):
		self.source_url = self.config['file_location']
		# Prepare subtable list for database.	
		self.statistics_db_value_list = []
	
	def extractData(self):
		data = {}
		# Open json file.
		in_file = open(self.config['file_location']).read()
		inp_data = json.loads(in_file)
		for entry in inp_data['machines']:
			# Resolve Nagios dictionary to three different entries.
			nagios_dict = entry.pop('nagios') 
			entry['nagios_link'] = nagios_dict['nagios_link']
			entry['nagios_warn'] = nagios_dict['nagios_warning']
			entry['nagios_crit'] = nagios_dict['nagios_critical']
			self.statistics_db_value_list.append(entry)
		return data
		
	def fillSubtables(self, parent_id):
		self.subtables['statistics'].insert().execute([dict(parent_id=parent_id, **row)
							       for row in self.statistics_db_value_list])

	# Making Subtable Data available to the html-output
	def getTemplateData(self):
		data = hf.module.ModuleBase.getTemplateData(self)
		details_list = self.subtables['statistics'].select().where(
		    self.subtables['statistics'].c.parent_id == self.dataset['id']).execute().fetchall()
		data["statistics"] = map(dict, details_list)
		return data

