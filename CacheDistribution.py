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
import numpy as np

import urllib2
import urllib
import itertools
import socket
import os
#from counter import Counter # using a backport of collections.Counter for 2.6 
from collections import Counter
import logging

class AutoVivification(dict):
    """Implementation of perl's autovivification feature."""
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value


class CacheDistribution(hf.module.ModuleBase):

    config_keys = {'sourceurl': ('Source Url', ''),
                   'plotsize_x': ('size of the plot in x', '10'),
                   'plotsize_y': ('size of plot in y', '5'),
                   'x_min': ('minimum for x ', '1'),
                   'x_max': ('maximum for x', '10000'),
                   'nbinsx': ('number of bins in x', '10'),
                   'nbinsy': ('number of bins in y', '10')
                   }
    table_columns = [
        Column('filename_plot', TEXT),
        Column('error_msg', TEXT),
        Column('datasets', INT),
        Column('failed_datasets', INT),
        Column('failed_machines', TEXT)
    ], ['filename_plot']

    def ideal_dist(self, x, n):
        try:
            dist =  np.sqrt(max(0.0, 1.0/x-1.0/n))
        except RuntimeWarning:
            dist = 0.0
	return dist

    def prepareAcquisition(self):
        link = self.config['sourceurl']
        self.plotsize_x = float(self.config['plotsize_x'])
        self.plotsize_y = float(self.config['plotsize_y'])
        self.nbinsx = int(self.config['nbinsx'])
        self.nbinsy = int(self.config['nbinsy'])
        self.x_min = float(self.config['x_min'])
        self.x_max = float(self.config['x_max'])
	
	self.logger = logging.getLogger(__name__) 
        self.machines = ['ekpsg01', 'ekpsg02', 'ekpsg03', 'ekpsg04', 'ekpsm01'] 
        self.in_data = {}
	
	def load_dslist(url):
            # to http requests
            req = urllib2.Request(url)
            try:
                response = urllib2.urlopen(req, timeout=2)
            # handle url error and timeout errors
            except urllib2.URLError as e:
                self.logger.error(str(e.reason) + " for " + url)
                return None
            except socket.timeout, e:
                self.logger.error("There was an error while reading " + url + ": %r" % e)
                return None
            except socket.timeout:
                self.logger.error("socket timeout for " + url)
                return None
            html = response.read()
            services = json.loads(html)
            dslist = list(itertools.chain(*services.values()))
            for k, entry in enumerate(dslist):
                dslist[k] = urllib.quote_plus(os.path.dirname(entry))
            # make list of unique datasets
            dataset = Counter(dslist)
            return dataset
	
	self.logger.info("Script to acquire datasets form Cache.")
        for machine in self.machines:
	    url = "http://" + machine + ".ekp.kit.edu:8080/cache/content/"
            error_count = 0
            self.in_data[machine] = {}
            self.logger.info("Reading detailed data from " + machine + '...')
            try:
                dslist = load_dslist(url).keys()
                dssize = load_dslist(url).values()
            except AttributeError:
                dslist = load_dslist(url)
                dssize = load_dslist(url)
            # handle error if dslist is empty
            if dslist == None:
                status = "Aquisition failed"
                self.logger.error(status + " for " + machine)
                dslist = []
                set_count = 0
            else:
                set_count = len(dslist)
                status = "Aquisition successful"
            # loop over every element in dslist
            for i, entry in enumerate(dslist):
                url += entry 
                # html request + error handling
                dsname = urllib.unquote_plus(entry)
                req = urllib2.Request(url)
                try:
                    response = urllib2.urlopen(req, timeout=1)
                    html = response.read()
                except urllib2.URLError as e:
                    self.logger.error("URLError" + dsname)
                    error_count += 1
                    continue
		except socket.timeout, e:
                    self.logger.error(("There was an error while reading the dataset details: %r" % e) + ": " + dsname)
                    error_count += 1
                    continue
                except socket.timeout:
                    self.logger.error("socket timeout for " + dsname)
                    error_count += 1
                    continue
                # load json file and dump data into lists
                services = json.loads(html)
                self.in_data[machine][dsname] = {
                    'size': services['size'],
                    'file_count': dssize[i],
                    'score': services['score']}
            self.logger.info("Dataset Details Completed")
        # generate Output file
            self.in_data[machine]['status'] = status
            self.in_data[machine]['ds_count'] = set_count
            self.in_data[machine]['error_count'] = error_count

	

    def extractData(self):
        import matplotlib.pyplot as plt
        data = {}
        data['filename_plot'] = ""
        data['error_msg'] = ""
        data['failed_machines'] = ""
        dataset = AutoVivification()
        machines = self.in_data.keys()
        for machine in machines:
            dsnames = self.in_data[machine].keys()
            dsnames.remove('status')
            dsnames.remove('error_count')
            dsnames.remove('ds_count')
            for name in dsnames:
                dataset[name][machine] = {
                    'size': self.in_data[machine][name]['size'],
                    'file_count': self.in_data[machine][name]['file_count']
                }
        status = list(self.in_data[id]['status'] for id in machines)
        ds_count = list(int(self.in_data[id]['ds_count']) for id in machines)
        error_count = list(int(self.in_data[id]['error_count']) for id in machines)
        removals = []
        for k in xrange(len(machines)):
            if "failed" in status[k] or ds_count[k]+error_count[k] == 0:
                removals.append(machines[k])
                data['error_msg'] = "Not all caches available - diagram may be wrong"
                data['status'] = 0.5
                data['failed_machines'] += machines[k] + " "
        for machine in removals:  # remove empty machines from dataset
            machines.remove(machine)
        for i in xrange(len(machines)):
            failed = 0
            if "failed" in status[i]:
                failed += 1
        if len(machines) == 0:
            data['status'] = 0
            data['error_msg'] = "No Cache available"
            return data
        if failed == len(machines):
            data['status'] = 0
            data['error_msg'] = "No data to display!"
            return data
        # calculate the metric of the Dataset
        # sum over all nodes - optimum minus real value, normed with 1-1/(number of nodes)
        metric = []
        file_count = []
        norm = np.sqrt(1-1.0/len(machines))
        for k in xrange(len(dataset.keys())):
            ds = dataset.keys()[k]
            temp = 0
            temp_2 = 0
            ds_total_size = sum(dataset[ds][id]['size'] for id in dataset[ds].keys())
            for j in xrange(len(machines)):
                try:
                    machine = dataset[ds].keys()[j]
                    size = dataset[ds][machine]['size']
                    temp += dataset[ds][machine]['file_count']
                except IndexError:
                    size = 0
                opt = (float(ds_total_size)*(1.0/(len(machines))))
                temp_2 += pow((float(opt - size)/ds_total_size), 2)
            metric.append(round((np.sqrt(temp_2)/norm), 3))
            file_count.append(temp)
        if sum(file_count) == 0:
            data['status'] = 0.5
            data['error_msg'] = "No files on caches found"
            return data
        ###############
        # Make   plot #
        ###############
        # calculate bin size so bins have equal size in log-Plot
        self.x_max = np.log10(self.x_max)
        self.x_min = np.log10(self.x_min)
        width = (self.x_max-self.x_min)/self.nbinsx
        xbins = []
        for k in xrange(self.nbinsx+1):
            xbins.append(pow(10, float(self.x_min + k*width)))
        ybins = np.arange(0.0, 1.001, 1.0/self.nbinsy)
        nbins = [xbins, ybins]
        H, xedges, yedges = np.histogram2d(file_count, metric, bins=nbins)
        fig = plt.figure(figsize=(self.plotsize_x, self.plotsize_y))
        H = np.rot90(H)
        H = np.flipud(H)
        plt.pcolor(xedges, yedges, H, cmap='Blues')
        cbar = plt.colorbar(ticks=np.arange(0, np.amax(H), 1))
        x = np.linspace(1, len(machines), 20*len(machines))
        y = map(lambda x: self.ideal_dist(x, len(machines)), x)
        plt.plot(x, y, linestyle='dotted')
        # cuten plot
        cbar.ax.set_ylabel('Counts')
        plt.ylabel('Metric')
        plt.xlabel('Number of Files')
        plt.title('Dataset Distribution')
        plt.xscale('log')
        plt.xlim(0.99, max(xedges))
        plt.ylim(0, 1)
        plt.yticks(yedges)
        plt.tight_layout()
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + "_filesize.png"), dpi=91)
        data["filename_plot"] = self.instance_name + "_filesize.png"
        data['datasets'] = len(file_count)
        data['failed_datasets'] = sum(error_count)
        return data
