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
from sqlalchemy import TEXT, Column
import json
import time
import socket
import logging
import urllib2
from ConfigParser import RawConfigParser

class Storages(hf.module.ModuleBase):
    config_keys = {'source_url': ('Source Url', 'http://monitor.ekp.kit.edu/ganglia/'),
		   'use_perc_warning': ('Lower treshold for use percentage of the disk space above which a warning is given', '80'),
                   'use_perc_critical': ('Lower treshold for use percentage of the disk space above which the status is critical', '90')
                  }
    table_columns = [], []
    subtable_columns = {
	'statistics' : ([
        	Column('Storage', TEXT),
        	Column('used', TEXT),
        	Column('total', TEXT),
		Column('use_perc', TEXT)], [])
    }

    
    def prepareAcquisition(self):
	# Setting defaults
	self.source_url = self.config['source_url']
        #prepare acqusition function.
        self.logger = logging.getLogger(__name__)
        self.storages = ['ekpfs7',
			 'ekpfs8',
			 'ekpfs9',
			 'ekpfsa',
			 'ekpfsb',
                         'ekpfsc'
                       ]
        self.infos = ['disk_total', 'disk_free']
        top_url = 'http://monitor.ekp.kit.edu/ganglia/'
	cfg_parser = RawConfigParser()
	cfg_parser.read('config/ganglia.cfg')
        username = cfg_parser.get('login', 'username')
        passwd = cfg_parser.get('login', 'passwd')
        pass_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        pass_mgr.add_password(None, top_url, username, passwd)
        authhandler = urllib2.HTTPBasicAuthHandler(pass_mgr)
	opener = urllib2.build_opener(authhandler)
	urllib2.install_opener(opener)
	
	# Prepare subtable list for database
        self.statistics_db_value_list = []
    
    def extractData(self):
	# Create data dictionary.
	data = {}
	entry_dict = {
		'Storage': 'Storage',
		'disk_total': 'total',
		'disk_free': 'available'
		}
        # Build url for all machines.
        for storage in self.storages:
	    storage_dict = {'Storage': storage}
            for info in self.infos:
                url = 'http://monitor.ekp.kit.edu/ganglia/graph.php?c=Storages&h=' \
                      + storage + '.ekp.kit.edu&m=' + info + '&vl=GB&ti=Total%20Disk%20Space&json=1'
                handle = urllib2.urlopen(url)
		html = handle.read()
		in_file = json.loads(html)[0]
		for datapoint in reversed(in_file['datapoints']):
			if datapoint[0] != 'NaN':
				storage_dict[entry_dict[info]] = datapoint[0] / 1024.
	    # Add a zero by hand to the dictionary if all values in json file are 'NaN'.
	    for value in entry_dict.itervalues():
		if value in storage_dict.keys():
			pass
		else:
			storage_dict[value] = 0.
	    storage_dict['used'] = float('{0:.2f}'.format(storage_dict['total'] - storage_dict['available']))
	    del storage_dict['available']
	    storage_dict['total'] = float('{0:.2f}'.format(storage_dict['total']))
	    if storage_dict['total'] == 0:
	    	storage_dict['use_perc'] = 0.0
	    else:
	        storage_dict['use_perc'] = int(storage_dict['used']/storage_dict['total']*100)
	    self.statistics_db_value_list.append(storage_dict)
	red_use = [1 for dictionary in self.statistics_db_value_list if dictionary['use_perc'] >= int(self.config['use_perc_critical'])]
	yellow_use = [1 for dictionary in self.statistics_db_value_list if dictionary['use_perc'] >= int(self.config['use_perc_warning'])]
	if red_use:
		data['status'] = 0.
	elif yellow_use:
		data['status'] = 0.5
	else:
		data['status'] = 1.
	# Loop over all entries in the statistics list and calculate the sum.
	sum_dict = {key: 0 for key in iter(storage_dict)}
	sum_dict['Storage'] = 'all'
	for dictionary in self.statistics_db_value_list:
		for key in iter(dictionary):
			if key != 'Storage' and key != 'use_perc':
				sum_dict[key] += dictionary[key]
        sum_dict['use_perc'] = int(sum_dict['used']/sum_dict['total']*100)
	sum_dict['used'] = float('{0:.2f}'.format(sum_dict['used']))
	sum_dict['total'] = float('{0:.2f}'.format(sum_dict['total']))
	self.statistics_db_value_list.append(sum_dict)
	return data	
				

    def fillSubtables(self, parent_id):
                self.subtables['statistics'].insert().execute([dict(parent_id=parent_id, **row) for row in self.statistics_db_value_list])

    def getTemplateData(self):  

                data = hf.module.ModuleBase.getTemplateData(self)
                statistics_list = self.subtables['statistics'].select().\
                        where(self.subtables['statistics'].c.parent_id == self.dataset['id']).execute().fetchall()
                data["statistics"] = map(dict, statistics_list)
                return data
