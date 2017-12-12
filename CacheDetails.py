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

class CacheDetails(hf.module.ModuleBase):
    config_keys = {'source_url': ('Not used, but filled to avoid warnings', 'http://ekpsg01.ekp.kit.edu:8080/cache/content/'),
                   'plotsize_x': ('size of the plot in x', '10'),
                   'plotsize_y': ('size of plot in y', '5'),
                   'score_limit': ('maximum score', '1000'),
                   'nbins': ('number of bins in histograms', '50')
                   }

    table_columns = [
        Column('filename_plot', TEXT),
        Column('error_msg', TEXT)
    ], ['filename_plot']

    subtable_columns = {
        'statistics': ([
            Column('machine', TEXT),
            Column('files', TEXT),
            Column('error_count', TEXT),
            Column('status', TEXT),
        ], []),
        'overscore': ([
            Column('filename', TEXT),
            Column('machine', TEXT),
            Column('score', TEXT),
            Column('size', TEXT)
        ], [])
        }

    def prepareAcquisition(self):
	# Setting defaults
	self.source_url = self.config["source_url"]
        self.plotsize_x = float(self.config['plotsize_x'])
        self.plotsize_y = float(self.config['plotsize_y'])
        self.nbins = float(self.config['nbins'])
        self.score_limit = int(self.config['score_limit'])
        self.statistics_db_value_list = []
        self.overscore_db_value_list = []
	
	self.logger = logging.getLogger(__name__)
	self.machines = ['ekpsg01', 'ekpsg02', 'ekpsg03', 'ekpsg04', 'ekpsm01']
        self.machine_data = {}


    def extractData(self):
	# read details for every file from filelist
        self.logger.info("Script to acquire details data form Cache.")
        for machine in self.machines:
            self.machine_data[machine] = {}
            file_count = 0
            status = ""
            self.logger.info("Reading detailed data from " + machine  + '...')
            url = "http://" + machine + ".ekp.kit.edu:8080/cache/content/*"
            # html request + error handling
            req = urllib2.Request(url)
            try:
                response = urllib2.urlopen(req, timeout=2)
                html = response.read()
                status = "Aquisition successful"
                services = json.loads(html)
                filenames = services.keys()
                file_count = len(filenames)
                for filename in filenames:
                    self.machine_data[machine][filename] = {
                        'size': services[filename]['size'],
                        'allocated': services[filename]['allocated'],
                        'score': services[filename]['score'],
                        'maintained': services[filename]['maintained']
                    }
                self.logger.info("Sucessful")
            except urllib2.URLError as e:
                self.logger.error(str(e.reason) + " " + machine)
                status = "Aquisition failed"
            except socket.timeout, e:
                self.logger.error(("There was an error while reading the file details: %r " % e) + machine)
                status = "Aquisition failed"
            except socket.timeout:
                self.logger.error("socket timeout " + machine)
                status = "Aquisition failed"
            # load json file and dump data into lists
            self.machine_data[machine]["status"] = status
            self.machine_data[machine]["file_count"] = file_count
            self.machine_data[machine]["error_count"] = 0		
	
        data = {}
        data['filename_plot'] = ""
        data['error_msg'] = ""
        self.plot_alloc = []
        self.plot_score = []
        self.plot_size = []
        self.plot_maint = []
        machines = self.machine_data.keys()
        for machine in machines:
            filenames = self.machine_data[machine].keys()
            filenames.remove('status')  # fix filenames list
            filenames.remove('file_count')
            filenames.remove('error_count')
            allocated = list(self.machine_data[machine][id]['allocated'] for id in filenames)
            allocated = filter(lambda x: x >= 0, allocated)
            alloc = list(map(lambda x: round((time.time()-float(x))/(60*60), 2), allocated))
            score = list(self.machine_data[machine][id]['score']for id in filenames)
            for k in xrange(len(score)):
                if score[k] is None:
                    score[k] = 0
            sizes = list(int(self.machine_data[machine][id]['size']) for id in filenames)
            maintained = list(self.machine_data[machine][id]['maintained']for id in filenames)
            maintained = filter(lambda x: x != 0, maintained)
            maint = list(map(lambda x: round((time.time()-float(x))/(60*60*24), 2), maintained))
            # find data with higher score than threshold in config and fill Subtable
            for k, filename in enumerate(filenames):
                if score[k] >= self.score_limit:
                    overscore = {
                        'filename': filename,
                        'score': score[k],
                        'machine': machine,
                        'size': round(float(sizes[k]/(1024*1024)), 1)
                    }
                    self.overscore_db_value_list.append(overscore)
            self.plot_size.append(list(map(lambda x: float(x)/(1024*1024), sizes)))
            self.plot_alloc.append(alloc)
            self.plot_score.append(score)
            self.plot_maint.append(maint)

        file_count = list(self.machine_data[id]['file_count']for id in machines)
        status = list(self.machine_data[id]['status'] for id in machines)
        error_count = list(self.machine_data[id]['error_count'] for id in machines)
        #  Error handling for acquisition of data
        for i, machine in enumerate(machines):
            details_data = {'machine': machine}
            details_data['files'] = file_count[i]
            details_data['error_count'] = error_count[i]
            failed = 0
            if "failed" in status[i]:
                details_data['status'] = 'data aquisition failed'
                data['status'] = 0.5
                failed += 1
            else:
                details_data['status'] = 'data aquisition successful'
            self.statistics_db_value_list.append(details_data)
        if failed == len(machines):
            data['status'] = 0
            data['error_msg'] = "No data to display!"
            return data
        if sum(file_count) == 0:
            data['status'] = 0.5
            data['error_msg'] = "No files on caches found"
            return data
        data["filename_plot"] = self.plot()
        return data

    def fillSubtables(self, parent_id):
        self.subtables['statistics'].insert().execute([dict(parent_id=parent_id, **row)
                                                       for row in self.statistics_db_value_list])
        self.subtables['overscore'].insert().execute([dict(parent_id=parent_id, **row)
                                                       for row in self.overscore_db_value_list])

    # Making Subtable Data available to the html-output
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['statistics'].select().where(
            self.subtables['statistics'].c.parent_id == self.dataset['id']).execute().fetchall()
        data["statistics"] = map(dict, details_list)
        details_list = self.subtables['overscore'].select().where(
            self.subtables['overscore'].c.parent_id == self.dataset['id']).execute().fetchall()
        data["overscore"] = map(dict, details_list)
        return data

    def plot(self):
        import matplotlib.pyplot as plt
        from matplotlib.font_manager import FontProperties
	
	# Create File Size Distribution plot.
        fig_file_size = plt.figure(figsize=(self.plotsize_x, self.plotsize_y))
        axis_file_size = fig_file_size.add_subplot(111)
        nbins = self.nbins
        fontLeg = FontProperties()
        fontLeg.set_size('small')
        # fix arrays so matplotlib 1.3.1 can plot the histograms
        machines_fix = []
        plot_fix = []
        for i in xrange(len(self.plot_size)):
            if len(self.plot_size[i]) != 0:
                plot_fix.append(self.plot_size[i])
                machines_fix.append(self.machines[i])
        axis_file_size.hist([plot_fix[i] for i in xrange(len(machines_fix))], nbins, histtype='bar', stacked=True)
        axis_file_size.legend(machines_fix, loc=6, bbox_to_anchor=(0.8, 0.88), borderaxespad=0., prop = fontLeg)
        axis_file_size.set_xlabel('FileSize in MiB')
        axis_file_size.set_ylabel('Number of Files')
        axis_file_size.set_title('File Size Distribution')
	# Create Allocation Time Distribution plot.
	fig_alloc = plt.figure(figsize=(self.plotsize_x, self.plotsize_y))
        axis_alloc = fig_alloc.add_subplot(111)
        # fix arrays so matplotlib 1.3.1 can plot the histograms
        machines_fix = []
        plot_fix = []
        for i in xrange(len(self.plot_alloc)):
            if len(self.plot_alloc[i]) != 0:
                plot_fix.append(self.plot_alloc[i])
                machines_fix.append(self.machines[i])
        axis_alloc.hist([plot_fix[i] for i in xrange(len(machines_fix))], nbins, histtype='bar', stacked=True, log=True)
        axis_alloc.legend(machines_fix, loc=6, bbox_to_anchor=(0.8, 0.88), borderaxespad=0., prop = fontLeg)
        axis_alloc.set_xlabel('Allocated since in hours')
        axis_alloc.set_ylabel('Number of Files')
        axis_alloc.set_title('Allocation Time Distribution')
	# Create Maintain Time Distribution.
	fig_maintain = plt.figure(figsize=(self.plotsize_x, self.plotsize_y))
        axis_maintain = fig_maintain.add_subplot(111)
        # fix arrays so matplotlib 1.3.1 can plot the histograms
        machines_fix = []
        plot_fix = []
        for i in xrange(len(self.plot_maint)):
            if len(self.plot_maint[i]) != 0:
                plot_fix.append(self.plot_maint[i])
                machines_fix.append(self.machines[i])
        axis_maintain.hist([plot_fix[i] for i in xrange(len(machines_fix))], nbins, histtype='bar', stacked=True, log=True)
        axis_maintain.legend(machines_fix, loc=6, bbox_to_anchor=(0.8, 0.88), borderaxespad=0., prop = fontLeg)
        axis_maintain.set_xlabel('Maintained since in days')
        axis_maintain.set_ylabel('Number of Files')
        axis_maintain.set_title('Maintain Time Distribution')
	# Create Score Distribution Plot.
	fig_score = plt.figure(figsize=(self.plotsize_x, self.plotsize_y))
        axis_score = fig_score.add_subplot(111)
        # fix arrays so matplotlib 1.3.1 can plot the histograms
        machines_fix = []
        plot_fix = []
        for i in xrange(len(self.plot_score)):
            if len(self.plot_score[i]) != 0:
                plot_fix.append(self.plot_score[i])
                machines_fix.append(self.machines[i])
        axis_score.hist([plot_fix[i] for i in xrange(len(machines_fix))], nbins, histtype='bar', stacked=True, log=True)
        axis_score.legend(machines_fix, loc=6, bbox_to_anchor=(0.8, 0.88), borderaxespad=0., prop=fontLeg)
        axis_score.set_xlabel('Score')
        axis_score.set_ylabel('Number of Files')
        axis_score.set_title('Score Distribution')
	# Save figures.
        #plt.tight_layout()
	plotname = hf.downloadService.getArchivePath(self.run, self.instance_name)
        fig_file_size.savefig(plotname + "_filesize.png", dpi=91)
        fig_alloc.savefig(plotname + "_allocation.png", dpi=91)
        fig_maintain.savefig(plotname + "_maintain.png", dpi=91)
        fig_score.savefig(plotname + "_score.png", dpi=91)
	
        return plotname 

