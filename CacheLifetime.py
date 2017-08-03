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

import datetime
import logging
import socket
import urllib2

class CacheLifetime(hf.module.ModuleBase):
    config_keys = {'sourceurl': ('Source Url', ''),
                   'plotsize_x': ('size of the plot in x', '10'),
                   'plotsize_y': ('size of plot in y', '5'),
                   'time_limit': ('in days max 30 days', '7'),
                   'nbins': ('number of bins in histograms', '200')
                   }
    table_columns = [
        Column('filename_plot', TEXT),
        Column('error_msg', TEXT)
    ], ['filename_plot']

    def prepareAcquisition(self):
        link = self.config['sourceurl']
        self.plotsize_x = float(self.config['plotsize_x'])
        self.plotsize_y = float(self.config['plotsize_y'])
        self.nbins = float(self.config['nbins'])
        self.time_limit = int(self.config['time_limit'])

	self.logger = logging.getLogger(__name__)
        self.inp_data = {}
        self.inp_data['error'] = ""
        self.date = time.time()-(2592000)  # tlimit = 2592000
        life_time = {'time': 0,
		     'life_time':0
		    }
        # function to load the filelists from ekpsg-machines
        url = "http://ekpsg03.ekp.kit.edu:8082/coordinator/stats/"
        # read data from every file in filelists
        self.logger.info("Script to acquire job and life_time information from coordinator.")
        urltotal = url + "life_time" + "?fields=" + \
            "&fields=".join(life_time.keys())  # build url for request
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
	self.inp_data['life_time'] = [] 
	for service in services:
	    if service[1] > int(self.date):
		self.inp_data[parameter].append(
					{
		    			 'time': service[1],
		    			 'life_time': service[0]
					}
				       )


    def extractData(self):
        import matplotlib.pyplot as plt
        from matplotlib.font_manager import FontProperties
        data = {}
        data['filename_plot'] = ""
        data['error_msg'] = ""
        if self.inp_data['error'] != "":
            data['status'] = 0
            data['error_msg'] = "Connection to Coordinator failed"
            return data
        time_list = list(int(entry['time']) for entry in self.inp_data['life_time'])
        lifetime_list = list(int(entry['life_time']) for entry in self.inp_data['life_time'])
        lifetime_list = map(lambda x: x/(60*60), lifetime_list)
        # TODO if data gets newer, constrain dataset von data from last 7 days etc.
        plot_lifetime_list = []
        for time in time_list:
            if time > time.time() - (self.time_limit*60*60*24):
                plot_lifetime_list.append(time)
        if len(plot_lifetime_list) == 0:
            data['status'] = 0.5
            data['error_msg'] = "No files removed in the last " + str(self.time_limit) + " days."
            print data
            return data
        fig = plt.figure(figsize=(self.plotsize_x, self.plotsize_y))
        axis = fig.add_subplot(111)
        nbins = self.nbins
        fontLeg = FontProperties()
        fontLeg.set_size('small')
        axis.hist(plot_lifetime_list, nbins, histtype='bar', log=True)
        axis.set_xlabel('Lifetime in hours')
        axis.set_ylabel('Number of Files')
        axis.set_title('Lifetime of Files in Cache')
        plt.tight_layout()
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + ".png"), dpi=91)
        data["filename_plot"] = self.instance_name + ".png"
        print data
        return data
