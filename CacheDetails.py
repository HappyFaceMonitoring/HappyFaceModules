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
	self.machines = ['epksg01', 'ekpsg02', 'ekpsg03', 'ekpsg04', 'ekpsm01']
        self.machine_data = {}


    def extractData(self):
        import matplotlib.pyplot as plt
        from matplotlib.font_manager import FontProperties
	
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
        plot_alloc = []
        plot_score = []
        plot_size = []
        plot_maint = []
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
            for k in xrange(len(filenames)):
                if score[k] >= self.score_limit:
                    overscore = {
                        'filename': filenames[k],
                        'score': score[k],
                        'machine': machine,
                        'size': round(float(sizes[k]/(1024*1024)), 1)
                    }
                    self.overscore_db_value_list.append(overscore)
            plot_size.append(list(map(lambda x: float(x)/(1024*1024), sizes)))
            plot_alloc.append(alloc)
            plot_score.append(score)
            plot_maint.append(maint)

        file_count = list(self.machine_data[id]['file_count']for id in machines)
        status = list(self.machine_data[id]['status'] for id in machines)
        error_count = list(self.machine_data[id]['error_count'] for id in machines)
        #  Error handling for acquisition of data
        for i in xrange(len(machines)):
            details_data = {'machine': machines[i]}
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
        ###############
        # Make   plot #
        ###############
        fig = plt.figure(figsize=(self.plotsize_x, self.plotsize_y*4))
        axis = fig.add_subplot(411)
        axis_2 = fig.add_subplot(412)
        axis_3 = fig.add_subplot(413)
        axis_4 = fig.add_subplot(414)
        nbins = self.nbins
        fontLeg = FontProperties()
        fontLeg.set_size('small')
        # fix arrays so matplotlib 1.3.1 can plot the histograms
        machines_fix = []
        plot_fix = []
        for i in xrange(len(plot_size)):
            if len(plot_size[i]) != 0:
                plot_fix.append(plot_size[i])
                machines_fix.append(machines[i])
        axis.hist([plot_fix[i] for i in xrange(len(machines_fix))], nbins, histtype='bar', stacked=True)
        axis.legend(machines_fix, loc=6, bbox_to_anchor=(0.8, 0.88), borderaxespad=0., prop = fontLeg)
        axis.set_xlabel('FileSize in MiB')
        axis.set_ylabel('Number of Files')
        axis.set_title('File Size Distribution')
        # fix arrays so matplotlib 1.3.1 can plot the histograms
        machines_fix = []
        plot_fix = []
        for i in xrange(len(plot_alloc)):
            if len(plot_alloc[i]) != 0:
                plot_fix.append(plot_alloc[i])
                machines_fix.append(machines[i])
        axis_2.hist([plot_fix[i] for i in xrange(len(machines_fix))], nbins, histtype='bar', stacked=True, log=True)
        axis_2.legend(machines_fix, loc=6, bbox_to_anchor=(0.8, 0.88), borderaxespad=0., prop = fontLeg)
        axis_2.set_xlabel('Allocated since in hours')
        axis_2.set_ylabel('Number of Files')
        axis_2.set_title('Allocation Time Distribution')
        # fix arrays so matplotlib 1.3.1 can plot the histograms
        machines_fix = []
        plot_fix = []
        for i in xrange(len(plot_maint)):
            if len(plot_maint[i]) != 0:
                plot_fix.append(plot_maint[i])
                machines_fix.append(machines[i])
        axis_3.hist([plot_fix[i] for i in xrange(len(machines_fix))], nbins, histtype='bar', stacked=True, log=True)
        axis_3.legend(machines_fix, loc=6, bbox_to_anchor=(0.8, 0.88), borderaxespad=0., prop = fontLeg)
        axis_3.set_xlabel('Maintained since in days')
        axis_3.set_ylabel('Number of Files')
        axis_3.set_title('Maintain Time Distribution')
        # fix arrays so matplotlib 1.3.1 can plot the histograms
        machines_fix = []
        plot_fix = []
        for i in xrange(len(plot_score)):
            if len(plot_score[i]) != 0:
                plot_fix.append(plot_score[i])
                machines_fix.append(machines[i])
        axis_4.hist([plot_fix[i] for i in xrange(len(machines_fix))], nbins, histtype='bar', stacked=True, log=True)
        axis_4.legend(machines_fix, loc=6, bbox_to_anchor=(0.8, 0.88), borderaxespad=0., prop = fontLeg)
        axis_4.set_xlabel('Score ')
        axis_4.set_ylabel('Number of Files')
        axis_4.set_title('Score Distribution')
        plt.tight_layout()
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + "_filesize.png"), dpi=91)
        data["filename_plot"] = self.instance_name + "_filesize.png"
        # fill subtables
        print "subtables:", data
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
