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
from sqlalchemy import INT, Column
import json
import time
import socket
import logging
import urllib2
from ConfigParser import RawConfigParser
import numpy as np

class CpuHours(hf.module.ModuleBase):
    config_keys = {'source_url' : ('Not used, but filled to avoid warnings', 'http://monitor.ekp.kit.edu/ganglia/')
                  }
    table_columns = [
	Column("Cpu_hours", INT),
	Column("empty_Cpu_hours", INT),
	Column("unused_resources", INT)
    ], []

    
    def prepareAcquisition(self):
	# Setting defaults
	self.source_url = self.config["source_url"]
        #prepare acqusition function.
        self.logger = logging.getLogger(__name__)
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
	
    
    def extractData(self):
	# Create data dictionary.
	data = {}
	# Set url of the plot in ganglia.
	url = "http://monitor.ekp.kit.edu/ganglia/graph.php?hreg[]=ekpcondorcentral.ekp.kit.edu" \
	      + "&mreg[]=BWFORCLUSTER-%28Cpus%28%3F%3ANot%29%3FInUse%29&vl=cores&aggregate=1&r=year&json=1"
	handle = urllib2.urlopen(url)
	html = handle.read()
	in_file = json.loads(html)
	# Create two arrays out of the datapoints with zeros instead of NaN.
	used_cpus = np.array([entry[0] if entry[0] != "NaN" else 0. for entry in in_file[0]["datapoints"]])
	not_used_cpus = np.array([entry[0] if entry[0] != "NaN" else 0. for entry in in_file[1]["datapoints"]])
	# Calculate hours per data point.
	hours_per_point = 365. / used_cpus.size * 24
	data["Cpu_hours"] = int(np.sum(used_cpus * hours_per_point))
	data["empty_Cpu_hours"] = int(np.sum(not_used_cpus * hours_per_point))
	data["unused_resources"] = int(float(data["empty_Cpu_hours"])/(data["Cpu_hours"] + data["empty_Cpu_hours"])*100)

	return data	
				
