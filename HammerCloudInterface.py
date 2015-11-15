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
from sqlalchemy import *
import lxml.html as lh
import urllib2 as ul

class HammerCloudInterface(hf.module.ModuleBase):
	config_keys = {
		'source_url' : ('HammerCloud URL', 'http://hc-ai-core.cern.ch/hc/app/cms/'),
		'sites_of_interest' : ('Names of the sites, for which you want to see currently running tests. Split the names by a semicolon','T1_DE_KIT'),
		'warning_threshold' : ('Percentage of efficiency, at which the module should have a warning status', '0.98'),
		'critical_threshold' : ('Percentage of efficiency, at which the module should have a critical status', '0.8')
	}
	table_columns = [], []
	subtable_columns = {
			'running_tests' : ( [
			Column('site_name', TEXT),
			Column('test_type', TEXT),
			Column('test_id', INT),
			Column('submitted_jobs', INT),
			Column('running_jobs', INT),
			Column('completed_jobs', INT),
			Column('failed_jobs', INT),
			Column('jobs_with_status_k', INT),
			Column('efficiency', FLOAT),
			Column('jobs_in_total', INT)
		], [] )
		}
	def prepareAcquisition(self):
		self.source_url = self.config['source_url']
		self.site_names = self.config['sites_of_interest'].split(";")
		self.running_tests_db_value_list = []
	def extractData(self):
		# parsing html page to find ID of tests on the site of interest
		data = {}
		hammercloud = lh.parse(self.source_url).getroot()
		testtypelist = hammercloud.findall(".//div[@class='runningjobs']") # extracting test types
		for site_name in self.site_names:
			for testtype in testtypelist:
				noentries = False
				plist = testtype.findall(".//p[@style]")
				for p in plist:
					if p.text == "No entries":
						noentries = True
						break
				if noentries: continue # if there are currently no tests for a certain test type, then no further search is done and the module continues with the next test type
				test_type_value = 'no information'
				test_id_value = 'no information'
				submitted_jobs_value = 'no information'
				running_jobs_value = 'no information'
				completed_jobs_value = 'no information'
				failed_jobs_value = 'no information'
				jobs_with_status_k_value = 'no information'
				efficiency_value = 'no information'
				jobs_in_total_value = 'no information'
				efficiency_status_list = []
				for test in testtype.findall(".//tr[@onmouseover]"):
					for td in test.findall(".//td"):
						if td.text.find(site_name) > -1: # looking for tests on the site of interest
							test_type_value = testtype.find(".//h3").text
							test_id_value = test.get("onclick").replace("DoNav('/hc/app/cms/test/","").replace("/');","") # retrieving ID of the test
							siteinfo = test.find(".//td[@style]").text
							site = site_name if (siteinfo.find(site_name) > -1) else "Anysite"
							json_url = self.source_url + "xhr/json/?action=results_at_site&test={IDNUMBER}&site={SITE}".format(IDNUMBER = test_id_value, SITE = site) # building appropriate .json url to get detailed information on the test
							readout = ul.urlopen(json_url).read()
							# finding and calculating several detailed information on the jobs of the test
							gsc = readout.count('"ganga_status": "c"')
							gss = readout.count('"ganga_status": "s"')
							gsk = readout.count('"ganga_status": "k"')
							gsr = readout.count('"ganga_status": "r"')
							gsf = readout.count('"ganga_status": "f"')
							submitted_jobs_value = gss
							running_jobs_value = gsr
							completed_jobs_value = gsc
							failed_jobs_value = gsf
							jobs_with_status_k_value = gsk
							efficiency_value = gsc/(1.*(gsf+gsc)) if gsf > 0 or gsc > 0 else 0
							jobs_in_total_value = gsc+gss+gsk+gsr+gsf

							if efficiency_value < float(self.config['critical_threshold']): efficiency_status_list.append(0.0)
							elif efficiency_value >= float(self.config['critical_threshold']) and efficiency_value < float(self.config['warning_threshold']): efficiency_status_list.append(0.5)
							else: efficiency_status_list.append(1.0)


							# passing the calculated values to the list used to fill the subtables
							cat_data = {
								'site_name' : site_name,
								'test_type' : test_type_value,
								'test_id' : test_id_value,
								'submitted_jobs' : submitted_jobs_value,
								'running_jobs' : running_jobs_value,
								'completed_jobs' : completed_jobs_value,
								'failed_jobs' : failed_jobs_value,
								'jobs_with_status_k': jobs_with_status_k_value,
								'efficiency' : round(efficiency_value,3),
								'jobs_in_total' : jobs_in_total_value
							}
							self.running_tests_db_value_list.append(cat_data)
		data['status'] = min(efficiency_status_list)
		return data
	def fillSubtables(self, module_entry_id):
		self.subtables['running_tests'].insert().execute([dict(parent_id=module_entry_id, **row) for row in self.running_tests_db_value_list])
	def getTemplateData(self):
		data = hf.module.ModuleBase.getTemplateData(self)
		running_tests_list = self.subtables['running_tests'].select().where(self.subtables['running_tests'].c.parent_id==self.dataset['id']).execute().fetchall()
		data['running_tests'] = map(dict, running_tests_list)
		return data
