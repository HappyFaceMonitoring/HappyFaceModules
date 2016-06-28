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
import numpy as np
from sqlalchemy import *

class DashboardSummary(hf.module.ModuleBase):
	
	config_keys = {
		'source_url' : ('Dashboard Summary URL', 'both||http://dashb-ssb.cern.ch/dashboard/request.py/getsiteplotdata?site=T1_DE_KIT&view=all&time=24&prettyprint=true'),
		'view_option' : ('View option', 'test'),
		'tier_name' : ('Name of the site', 'T1_DE_KIT')
	}
	
	table_columns = [],[]
	subtable_columns = {
		'latest_data' : ( [
		Column('metric_name',TEXT),
		Column('latest_status',INT),
		Column('latest_time', INT),
	], [] ),
		'history_data' : ( [
		Column('metric_name',TEXT),
		Column('status', INT),
		Column('time',INT)
	], [] ),
		'formatted_history_data' : ( [
		Column('metric_name',TEXT),
		Column('status', INT),
		Column('time',INT)
	], [] )
	}
	
	def prepareAcquisition(self):
		# Updating the url for a chosen view option and tier site. Possible view options:
		# default, maint, storage, test, transfers, Good links, Active Links, 
		# Further possiblities see for example on http://dashb-ssb.cern.ch/dashboard/request.py/sitehistory?site=T1_DE_KIT
		url = self.config['source_url']
		old_view_option = url[url.find("view"):url.find("time")-1]
		old_tier_name = url[url.find("?site"):url.find("view")-1]
		view_option_string = "view=" + self.config['view_option']
		tier_name_string = "?site=" + self.config['tier_name']
		self.config['source_url'] = self.config['source_url'].replace(old_view_option,view_option_string).replace(old_tier_name, tier_name_string)
		try:
			self.source = hf.downloadService.addDownload(self.config['source_url'])
			self.source_url = self.source.getSourceUrl()
		except:
			raise hf.exceptions.ConfigError('Not possible to set the source')
		
		self.latest_data_db_value_list = []
		self.history_data_db_value_list = []
		self.formatted_data_db_value_list = []
		self.plots_list = []
		
	def extractData(self):
		import matplotlib.pyplot as plt
		data = {}
		status_list = []
		latest_status_list =[]
		with open(self.source.getTmpPath(), 'r') as f:
			data_object = json.loads(f.read())
		
		for index, metric in enumerate(data_object['data']):
			info_latest = {'metric_name':metric[0],'latest_status':-1,'latest_time':-1}
			info_allday_list = []
			info_formatted_history_list = []
			
			# going through entries in data for the metric. Format of datapoint:
			# [time in seconds, status in integers (coding see source_url)]
			for datapoint in reversed(metric[1]):
				info = {'metric_name':metric[0],'status':-1,'time':-1}
				info_formatted = {'metric_name':metric[0],'status':-1,'time':-1}
				
				info['time'],info['status'] = datapoint
				info_formatted['time'] = datapoint[0]
				if datapoint[1] == 5: info_formatted['status'] = 4
				elif datapoint[1] == 4: info_formatted['status'] = 3
				elif datapoint[1] == 3: info_formatted['status'] = 2
				elif datapoint[1] == 8: info_formatted['status'] = 0
				else: info_formatted['status'] = 1
				
				# determining latest status for metric. Status 8 means, that it is not filled for that time.
				if info_latest['latest_time'] == -1 and datapoint[1] != 8:
					info_latest['latest_time'],info_latest['latest_status'] = datapoint
				
				info_allday_list.insert(0,info)
				info_formatted_history_list.insert(0,info_formatted)
			
			if len(info_allday_list) == 0: continue
			self.latest_data_db_value_list.append(info_latest)
			self.history_data_db_value_list += info_allday_list
			self.formatted_data_db_value_list += info_formatted_history_list
			
			# prepare lists to determine status
			if info_latest['latest_status'] == 3: latest_status_list.append(0.0)
			elif info_latest['latest_status'] == 4: latest_status_list.append(0.5)
			elif info_latest['latest_status'] == 5: latest_status_list.append(1.0)
		
		# Save determined status
		data['status'] = min(latest_status_list)
		
		return data
	
	def fillSubtables(self, module_entry_id):
		self.subtables['latest_data'].insert().execute([dict(parent_id=module_entry_id, **row) for row in self.latest_data_db_value_list])
		self.subtables['history_data'].insert().execute([dict(parent_id=module_entry_id, **row) for row in self.history_data_db_value_list])
		self.subtables['formatted_history_data'].insert().execute([dict(parent_id=module_entry_id, **row) for row in self.formatted_data_db_value_list])
	def getTemplateData(self):
		data = hf.module.ModuleBase.getTemplateData(self)
		latest_data_list = self.subtables['latest_data'].select().where(self.subtables['latest_data'].c.parent_id==self.dataset['id']).execute().fetchall()
		history_data_list = self.subtables['history_data'].select().where(self.subtables['history_data'].c.parent_id==self.dataset['id']).execute().fetchall()
		formatted_history_data_list = self.subtables['formatted_history_data'].select().where(self.subtables['formatted_history_data'].c.parent_id==self.dataset['id']).execute().fetchall()
		
		data['latest_data'] = map(dict, latest_data_list)
		data['history_data'] = map(dict, history_data_list)
		data['formatted_history_data'] = map(dict, formatted_history_data_list)
		data['view_option'] = self.config['view_option']
		data['tier_name'] = self.config['tier_name']
		data['link_url'] =  'http://dashb-ssb.cern.ch/dashboard/request.py/sitehistory?site={SITE}#currentView={VIEW}&time=24&start_date=&end_date=&values=false&spline=false&white=false'.format(SITE=self.config['tier_name'], VIEW=self.config['view_option'])
		return data
