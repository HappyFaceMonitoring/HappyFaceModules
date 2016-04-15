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

class PendingPhedexTransferRequests(hf.module.ModuleBase):
	
	config_keys = {
		'source_url' : ('Phedex Transfer Requests URL', 'both||https://cmsweb.cern.ch/phedex/datasvc/json/prod/RequestList?decision=pending&node=T1_DE_KIT*'),
		'tier_name' : ('Name of the site', 'T1_DE_KIT'),
		'time_warning_threshold' : ('Maximal time in hours requests are allowed to exist without "warning" status.', '3'),
		'time_critical_threshold' : ('Maximal time in hours requests are allowed to exist without "critical" status.', '23')
	}
	
	table_columns = [
		Column("tier_name", TEXT),
		Column("pending_transfer_requests", INT),
		Column("max_request_time", FLOAT)
		],[]
	
	def prepareAcquisition(self):
		
		# Updating the url for chosen tier site.
		url = self.config['source_url']
		url = url[:url.find("RequestList?")+len("RequestList?")]
		url += "decision=pending&node={TIER}*".format(TIER=self.config["tier_name"])
		self.config['source_url'] = url
		try:
			self.source = hf.downloadService.addDownload(self.config['source_url'])
			self.source_url = self.source.getSourceUrl()
		except:
			raise hf.exceptions.ConfigError('Not possible to set the source')
	
	def extractData(self):
		
		data = {"tier_name":self.config["tier_name"]}
		
		with open(self.source.getTmpPath(), 'r') as f:
			request_list = json.loads(f.read())
		
		current_time = time.time()
		request_time_list = [0.0]
		for request in request_list["phedex"]["request"]:
			request_time = time.gmtime(request["time_create"])
			period = current_time - request["time_create"]
			request_time_list.append(period)
		data["pending_transfer_requests"] = len(request_time_list)-1
		data["max_request_time"] = round(max(request_time_list)/3600,1)
		
		if data["max_request_time"] > float(self.config["time_critical_threshold"]): data["status"] = 0.0
		elif data["max_request_time"] >  float(self.config["time_warning_threshold"]): data["status"] = 0.5
		else: data["status"] = 1.0
		
		return data
