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

class Portals(hf.module.ModuleBase):
    config_keys = {'sourceurl': ('Source Url', '')
                  }
    table_columns = [], []
    subtable_columns = {
	'statistics' : ([
        	Column('Portal', TEXT),
        	Column('use', TEXT),
        	Column('share', TEXT),
        	Column('CPUs', TEXT),
        	Column('1-min', TEXT)], [])
    }

    
    def prepareAcquisition(self):
        #prepare acqusition function.
        self.logger = logging.getLogger(__name__)
        self.portals = ['ekpams2', 'ekpams3',
                        'ekpbms1', 'ekpbms2', 'ekpbms3',
                        'ekpcms5', 'ekpcms6'
                       ]
        self.infos = ['mem', 'load']
        top_url = 'http://ekpmonitor.ekp.kit.edu/ganglia/'
	cfg_parser = RawConfigParser()
        cfg_parser.read('ganglia.cfg')
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
		'Portal': 'Portal',
		'Use\\g': 'use',
		'Share\\g': 'share',
		'CPUs ': 'CPUs',
		'1-min': '1-min'
		}
        # Build url for all machines.
        for portal in self.portals:
	    portal_dict = {'Portal': portal}
            for info in self.infos:
                url = 'http://ekpmonitor.ekp.kit.edu/ganglia/graph.php?h=' \
                      + portal + '.ekp.kit.edu&m=load_one&r=hour&s=by%20name&hc=4&mc=2&g=' \
                      + info + '_report&z=large&c=Portals&json=1'
                handle = urllib2.urlopen(url)
		html = handle.read()
		in_file = json.loads(html)
		for entry in in_file:
			if entry['metric_name'] in entry_dict.keys():
				for datapoint in reversed(entry['datapoints']):
					if datapoint[0] != 'NaN':
						portal_dict[entry_dict[entry['metric_name']]] = datapoint[0]
	    # Add a zero by hand to the dictionary if all values in json file are 'Nan'.
	    for value in entry_dict.itervalues():
		if value in portal_dict.keys():
			pass
		else:
			portal_dict[value] = 0
	    portal_dict['use'] /= 1024.**3
	    portal_dict['share'] /= 1024.**3
	    portal_dict['use'] = float("{0:.2f}".format(portal_dict['use']))
	    portal_dict['share'] = float("{0:.2f}".format(portal_dict['share']))
	    portal_dict['1-min'] = float("{0:.2f}".format(portal_dict['1-min']))
	    self.statistics_db_value_list.append(portal_dict)
	# Loop over all entries in the statistics list and calculate the sum.
	sum_dict = {key: 0 for key in iter(portal_dict)}
	sum_dict['Portal'] = 'all'
	for dictionary in self.statistics_db_value_list:
		for key in iter(dictionary):
			if key != 'Portal':
				sum_dict[key] += dictionary[key]
	self.statistics_db_value_list.append(sum_dict)
	print self.statistics_db_value_list
	return data	
				

    def fillSubtables(self, parent_id):
                self.subtables['statistics'].insert().execute([dict(parent_id=parent_id, **row) for row in self.statistics_db_value_list])

    def getTemplateData(self):  

                data = hf.module.ModuleBase.getTemplateData(self)
                statistics_list = self.subtables['statistics'].select().\
                        where(self.subtables['statistics'].c.parent_id == self.dataset['id']).execute().fetchall()
                data["statistics"] = map(dict, statistics_list)
                return data
