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
from sqlalchemy import TEXT, INT, Column
import json

import urllib2
import socket
import logging


class CacheSummary(hf.module.ModuleBase):
    config_keys = {'sourceurl': ('Source Url', '')
                   }
    table_columns = [
        Column('size', INT),
        Column('avail', INT),
        Column('used', INT),
        Column('score', INT),
        Column('error_msg', TEXT),
        Column('error', INT),
        Column('total_files', INT)
    ], []
    

    def prepareAcquisition(self):
	self.logger = logging.getLogger(__name__)
        self.machines = ['epksg01', 'ekpsg02', 'ekpsg03', 'ekpsg04', 'ekpsm01']
        self.summary_url = ['http://ekpsg01.ekp.kit.edu:8080/cache/backends/cache',
			    'http://ekpsg02.ekp.kit.edu:8080/cache/backends/cache',
			    'http://ekpsg03.ekp.kit.edu:8080/cache/backends/cache',
			    'http://ekpsg04.ekp.kit.edu:8080/cache/backends/cache',
			    'http://ekpsm01.ekp.kit.edu:8080/cache/backends/cache']
        self.summary_data = {}

    def extractData(self):
	# generate data for summary module
        self.logger.info("Script to acquire summary data form Cache.")
        for i in xrange(len(self.summary_url)):
            # handle url request + errors
            self.logger.info("Reading summary data from " + self.machines[i] + '...')
            req = urllib2.Request(self.summary_url[i])
            try:
                response = urllib2.urlopen(req, timeout=2)
                html = response.read()
            except urllib2.URLError as e:
                self.logger.error(str(e.reason) + ' ' + self.summary_url[i])
                self.summary_data[self.machines[i]] = 'no data'
                continue
            except socket.timeout, e:
                self.logger.error("There was an error while reading " + self.summary_url[i] + ": %r" % e)
                self.summary_data[self.machines[i]] = 'no data'
                continue
            except socket.timeout:
                self.logger.error("socket timeout" + ' ' + self.summary_url[i])
                self.summary_data[self.machines[i]] = 'no data'
                continue
            html_fix = html.replace("Infinity", "0")  # fix unreadable value in dict
            services = json.loads(html_fix)[0]
            self.summary_data[self.machines[i]] = services  # save data in dict
            self.logger.info("Sucessful")
	
        data = {}
        data['error_msg'] = 0
        data['error'] = 0
        volume_size, volume_used, file_number, score_average, volume_avail = [], [], [], [], []
	ekpsg = list(self.summary_data.keys())
	for Id in ekpsg:
	    if self.summary_data[Id] != 'no data':
		volume_size.append(int(self.summary_data[Id]['volume']['total']))
		volume_avail.append(int(self.summary_data[Id]['volume']['avail']))
		volume_used.append(int(self.summary_data[Id]['volume']['used']))
		file_number.append(int(self.summary_data[Id]['allocation']['files_total']))
		score_average.append(float(self.summary_data[Id]['allocation']['score_average']))
	    else:
	        data['error'] += 1
        data['size'] = sum(volume_size)/(1024*1024*1024)
        data['avail'] = sum(volume_avail)/(1024*1024*1024)
        data['used'] = sum(volume_used)/(1024*1024*1024)
        if data['error'] < 4:
            try:
                data['score'] = round(sum(score_average)/len(score_average), 2)
            except ZeroDivisionError:
                data['score'] = 0
        else:
            data['error_msg'] = "No data to display!"
            data['status'] = 0
        data['total_files'] = sum(file_number)
        return data
