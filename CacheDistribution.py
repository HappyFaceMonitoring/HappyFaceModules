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
from sqlalchemy import *
import json
import numpy as np

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
        return np.sqrt(max(0.0, 1.0/x-1.0/n))

    def prepareAcquisition(self):
        link = self.config['sourceurl']
        self.plotsize_x = float(self.config['plotsize_x'])
        self.plotsize_y = float(self.config['plotsize_y'])
        self.nbinsx = int(self.config['nbinsx'])
        self.nbinsy = int(self.config['nbinsy'])
        self.x_min = float(self.config['x_min'])
        self.x_max = float(self.config['x_max'])
        # Download the file
        self.source = hf.downloadService.addDownload(link)
        # Get URL
        self.source_url = self.source.getSourceUrl()

    def extractData(self):
        import matplotlib.pyplot as plt
        data = {}
        data['filename_plot'] = ""
        data['error_msg'] = ""
        data['failed_machines'] = ""
        path = self.source.getTmpPath()
        # open file
        dataset = AutoVivification()
        with open(path, 'r') as f:
            # fix the JSON-File, so the file is valid
            content = f.read()
            services = json.loads(content)
        machines = services.keys()
        for machine in machines:
            dsnames = services[machine].keys()
            dsnames.remove('status')
            dsnames.remove('error_count')
            dsnames.remove('ds_count')
            for name in dsnames:
                dataset[name][machine] = {
                    'size': services[machine][name]['size'],
                    'file_count': services[machine][name]['file_count']
                }
        status = list(services[id]['status'] for id in machines)
        ds_count = list(int(services[id]['ds_count']) for id in machines)
        error_count = list(int(services[id]['error_count']) for id in machines)
        removals = []
        for k in xrange(len(machines)):
            if "failed" in status[k] or ds_count[k]+error_count[k] == 0:
                removals.append(machines[k])
                data['error_msg'] = "Not all caches available - diagram may be wrong"
                data['status'] = 0.5
                data['failed_machines'] += machines[k] + " "
        for machine in removals:  # remove empty machines from dataset
            machines.remove(machine)
        """ calculate the metric of the Dataset
            sum over all nodes - optimum minus real value, normed with 1-1/(number of nodes)"""
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
        for i in xrange(len(machines)):
            failed = 0
            if "failed" in status[i]:
                failed += 1
        if failed == len(machines):
            data['status'] = 0
            data['error_msg'] = "No data to display!"
            print data
            return data
        if sum(file_count) == 0:
            data['status'] = 0.5
            data['error_msg'] = "No files on caches found"
            print data
            return data
        ###############
        # Make   plot #
        ###############
        ''' calculate bin size so bins have equal size in log-Plot '''
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
        x = np.linspace(0, len(machines), 20*len(machines))
        y = map(lambda x: self.ideal_dist(x, len(machines)), x)
        plt.plot(x, y)
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
        print data
        return data
