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
    config_keys = {'source_url': ('Not used, but filled to avoid warnings', 'http://monitor.ekp.kit.edu/ganglia/'),
		   'use_perc_warning': ('Lower treshold for use percentage of the disk space above which a warning is given', '80'),
		   'use_perc_critical': ('Lower treshold for use percentage of the disk space above which the status is critical', '90')
                  }
    table_columns = [], []
    subtable_columns = {
	'statistics' : ([
        	Column('Portal', TEXT),
        	Column('use', TEXT),
        	Column('total', TEXT),
		Column('use_perc', TEXT),
        	Column('CPUs', TEXT),
        	Column('1-min', TEXT)], [])
    }

    
    def prepareAcquisition(self):
	# Setting defaults
	self.source_url = self.config["source_url"]
        #prepare acqusition function.
        self.logger = logging.getLogger(__name__)
        self.portals = ['ekpams2', 'ekpams3',
                        'ekpbms1', 'ekpbms2', 'ekpbms3',
                        'ekpcms5', 'ekpcms6'
                       ]
        self.infos = ['mem', 'load']
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
		'Portal': 'Portal',
		'Use\\g': 'use',
		'Total\\g': 'total',
		'CPUs ': 'CPUs',
		'1-min': '1-min'
		}
        # Build url for all machines.
        for portal in self.portals:
	    portal_dict = {'Portal': portal}
            for info in self.infos:
                url = 'http://monitor.ekp.kit.edu/ganglia/graph.php?h=' \
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
	    portal_dict['total'] /= 1024.**3
	    portal_dict['use'] = float("{0:.2f}".format(portal_dict['use']))
	    portal_dict['total'] = float("{0:.2f}".format(portal_dict['total']))
	    portal_dict['1-min'] = float("{0:.2f}".format(portal_dict['1-min']))
	    try:
		    portal_dict['use_perc'] = int(portal_dict['use'] / portal_dict['total'] * 100)
	    except ZeroDivisionError:
		    portal_dict['use_perc'] = 100
	    self.statistics_db_value_list.append(portal_dict)
	# Loop over all entries in the statistics list and calculate the sum.
	sum_dict = {key: 0 for key in iter(portal_dict)}
	sum_dict['Portal'] = 'all'
	for dictionary in self.statistics_db_value_list:
		for key in iter(dictionary):
			if key != 'Portal' and key != 'use_perc':
				sum_dict[key] += dictionary[key]
	sum_dict['total'] = float("{0:.2f}".format(sum_dict['total']))
	sum_dict['use'] = float("{0:.2f}".format(sum_dict['use']))
	sum_dict['1-min'] = round(sum_dict['1-min'],2)
	red_1min = [1 for dictionary in self.statistics_db_value_list if dictionary['1-min'] >= dictionary['CPUs']] 
	yellow_1min = [1 for dictionary in self.statistics_db_value_list if dictionary['1-min'] >= 0.75 * dictionary['CPUs']] 
	red_use = [1 for dictionary in self.statistics_db_value_list if dictionary['use_perc'] >= self.config['use_perc_critical']]
	yellow_use = [1 for dictionary in self.statistics_db_value_list if dictionary['use_perc'] >= self.config['use_perc_warning']]
	if red_1min or red_use:
		data['status'] = 0.
	elif yellow_1min or yellow_use:
		data['status'] = 0.5
	else:
		data['status'] = 1. 
	sum_dict['use_perc'] = int(sum_dict['use'] / sum_dict['total'] * 100)
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
