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

import logging
import socket
import datetime
import urllib2

class CacheHitMiss(hf.module.ModuleBase):
    config_keys = {'sourceurl': ('Source Url', ''),
                   'plotsize_x': ('size of the plot in x', '8.9'),
                   'plotsize_y': ('size of plot in y', '5'),
                   'time_limit': ('in days max 30 days', '7'),
                   'nbins': ('number of bins in histograms', '5')
                   }
    table_columns = [
        Column('filename_plot', TEXT),
        Column('error_msg', TEXT)
    ], ['filename_plot']
    

    def prepareAcquisition(self):
        link = self.config['sourceurl']
        self.plotsize_x = float(self.config['plotsize_x'])
        self.plotsize_y = float(self.config['plotsize_y'])
        self.nbins = int(self.config['nbins'])
        self.time_limit = int(self.config['time_limit'])
        self.time_limits = time.time() - self.time_limit*24*60*60

	self.logger = logging.getLogger(__name__)
        self.inp_data = {}
	self.inp_data['error'] = ""
	self.date = time.time()-(2592000)  # tlimit = 2592000
	jobs = {'creation_time': 0,
                'locality_rate': 0,
	        'node_id': 0,
                'cachehit_rate': 0
               }
	# function to load the filelists from ekpsg-machines
        url = "http://ekpsg03.ekp.kit.edu:8082/coordinator/stats/"
        # read data from every file in filelists
        self.logger.info("Script to acquire job and life_time information from coordinator.")
	urltotal = url + "jobs" + "?fields=" + \
	    "&fields=".join(jobs.keys())  # build url for request
	self.logger.info("url: " + urltotal)
	req = urllib2.Request(urltotal)
	try:
	    response = urllib2.urlopen(req, timeout=30)
	    # handle url error and timeout errors
	except urllib2.URLError as e:
	    self.logger.error(e.reason)
	    self.inp_data['error'] += " Connection problems"
	except socket.timeout, e:
	    self.logger.error("There was an error while reading " + url + ": %r" % e)
	    self.inp_data['error'] += " Connection problems"
	except socket.timeout:
	    self.logger.error("socket timeout")
	    self.inp_data['error'] += " Connection problems"
	html = response.read()
	services = json.loads(html)
	self.inp_data["jobs"] = []
	for service in services:
	    if service[2] > int(self.date):
		self.inp_data["jobs"].append(
					    {'creation_time': service[2],
					     'locality_rate': service[0],
					     'cachehit_rate': service[3]
					    }
				   )


    def extractData(self):
        import matplotlib.pyplot as plt
        import numpy as np
        data = {}
        data['filename_plot'] = ""
        data['error_msg'] = ""
        if self.inp_data['error'] != "":
            data['status'] = 0
            data['error_msg'] = "Connection to Coordinator failed"
            return data
        hit_list = []
        local_list = []
        for entry in self.inp_data['jobs']:
            if entry['creation_time'] > self.time_limits:
                hit_list.append(float(entry['cachehit_rate']))
                local_list.append(float(entry['locality_rate']))
        # generate 2d histogram
        nbins = 1.0/(self.nbins)
        bins = [np.arange(0.0, 1.1, nbins), np.arange(0.0, 1.1, nbins)]
        H, xedges, yedges = np.histogram2d(hit_list, local_list, bins=bins)
        fig = plt.figure(figsize=(self.plotsize_x, self.plotsize_y))
        H = np.rot90(H)
        H = np.flipud(H)
        plt.pcolor(xedges, yedges, H, cmap='Blues')
        cbar = plt.colorbar()
        cbar.ax.set_ylabel('Jobs')
        plt.ylabel('locality rate')
        plt.xlabel('cachehit rate')
        plt.title('Cache Hit Distribution for the last ' + str(self.time_limit) + " days")
        plt.tight_layout()
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + "_filesize.png"), dpi=91)
        data["filename_plot"] = self.instance_name + "_filesize.png"
        print data
        return data
