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
from sqlalchemy import TEXT, INT, FLOAT, Column
import numpy as np
import htcondor

class HealthyNodes(hf.module.ModuleBase):
	
	
	config_keys = {
			'source_url': ('Source URL', ''),
			}
	
	table_columns = [], []
	
	subtable_columns = {
			'statistics' : ([
				Column('machine', TEXT),
				Column('message', TEXT)], [])
			   }
	
	def prepareAcquisition(self):
		# Define basic structures
                self.condor_projection = [
			'NODE_IS_HEALTHY',
			'Machine'
		]
		
		# Prepare htcondor queries
                self.collector = htcondor.Collector()
		self.requirement = '( CLOUDSITE=="condocker" || CLOUDSITE=="ekpsupermachines" ) && SlotTypeID == 1'
			
		# Prepare subtable list for database
                self.statistics_db_value_list = []
	
	def extractData(self):
		data = {}
		result = self.collector.query(htcondor.AdTypes.Startd, self.requirement, self.condor_projection)
		for node in result:
			node_dict = {}
			if node['NODE_IS_HEALTHY'] == True or node['NODE_IS_HEALTHY'] == 'undefined':
				pass
			else:
				node_dict['message'] = node['NODE_IS_HEALTHY']
				node_dict['machine'] = node['Machine']
			# Save only filled dictionaries.
			if len(node_dict) == 0:
				pass
			else:
				self.statistics_db_value_list.append(node_dict)
		return data
					
		
        def fillSubtables(self, parent_id):
                self.subtables['statistics'].insert().execute([dict(parent_id=parent_id, **row) for row in self.statistics_db_value_list])


        def getTemplateData(self):

                data = hf.module.ModuleBase.getTemplateData(self)
                statistics_list = self.subtables['statistics'].select().\
                        where(self.subtables['statistics'].c.parent_id == self.dataset['id']).execute().fetchall()
                data['statistics'] = map(dict, statistics_list)
                return data
