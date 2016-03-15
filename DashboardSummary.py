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
import json
import time
from sqlalchemy import *

class DashboardSummary(hf.module.ModuleBase):
	
	config_keys = {
		'source_url' : ('Dashboard Summary URL', 'both||http://dashb-ssb.cern.ch/dashboard/request.py/getsiteplotdata?site=T1_DE_KIT&view=all&time=24&prettyprint=true'),
		'view_option' : ('View option', 'test'),
	}
	
	table_columns = [], []
	subtable_columns = {
		'latest_data' : ( [
		Column('metric_name',TEXT),
		Column('latest_status',INT),
		Column('latest_time', INT)
	], [] ),
		'allday_data' : ( [
		Column('metric_name',TEXT),
		Column('status', INT),
		Column('time',INT)
	], [] )
	}
	
	def prepareAcquisition(self):
		# Updating the url for a chosen view option. Possible options:
		# default, maint, storage, test, transfers
		# Further possiblities see on http://dashb-ssb.cern.ch/dashboard/request.py/sitehistory?site=T1_DE_KIT
		url = self.config['source_url']
		old_view_option = url[url.find("view"):url.find("time")-1]
		view_option_string = "view=" + self.config['view_option']
		self.config['source_url'] = self.config['source_url'].replace(old_view_option,view_option_string)
		try:
			self.source = hf.downloadService.addDownload(self.config['source_url'])
			self.source_url = self.source.getSourceUrl()
		except:
			raise hf.exceptions.ConfigError('Not possible to set the source')
		
		self.latest_data_db_value_list = []
		self.allday_data_db_value_list = []
		
	def extractData(self):
		data = {}
		status_list = []
		with open(self.source.getTmpPath(), 'r') as f:
			data_object = json.loads(f.read())
		
		for metric in data_object['data']:
			info_latest = {'metric_name':metric[0],'latest_status':-1,'latest_time':-1}
			info_allday_list = []
			
			# going through entries in data for the metric. Format of datapoint:
			# [time in seconds, status in integers (coding see source_url)]
			for datapoint in reversed(metric[1]):
				info = {'metric_name':metric[0],'status':-1,'time':-1}
				info['time'],info['status'] = datapoint
				
				# determining latest status for metric. Status 8 means, that it is not filled for that time.
				if info_latest['latest_time'] == -1 and datapoint[1] != 8:
					info_latest['latest_time'],info_latest['latest_status'] = datapoint
				
				info_allday_list.insert(0,info)
			
			self.latest_data_db_value_list.append(info_latest)
			self.allday_data_db_value_list += info_allday_list
			
			if info_latest['latest_status'] == 3: status_list.append(0.0)
			elif info_latest['latest_status'] == 4: status_list.append(0.5)
			elif info_latest['latest_status'] == 5: status_list.append(1.0)
		
		data['status'] = min(status_list)
		return data
	
	def fillSubtables(self, module_entry_id):
		self.subtables['latest_data'].insert().execute([dict(parent_id=module_entry_id, **row) for row in self.latest_data_db_value_list])
		self.subtables['allday_data'].insert().execute([dict(parent_id=module_entry_id, **row) for row in self.allday_data_db_value_list])
	def getTemplateData(self):
		data = hf.module.ModuleBase.getTemplateData(self)
		latest_data_list = self.subtables['latest_data'].select().where(self.subtables['latest_data'].c.parent_id==self.dataset['id']).execute().fetchall()
		allday_data_list = self.subtables['allday_data'].select().where(self.subtables['allday_data'].c.parent_id==self.dataset['id']).execute().fetchall()
		data['latest_data'] = map(dict, latest_data_list)
		data['allday_data'] = map(dict, allday_data_list)
		data['view_option'] = self.config['view_option']
		return data
