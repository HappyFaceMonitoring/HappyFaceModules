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
import time
import datetime
import ast


class CMS6History(hf.module.ModuleBase):
    config_keys = {'sourceurl': ('Source Url', ''),
                   'plotrange': ('number of hours in plot (maximum is 24)', '24'),
                   'plotsize_x': ('size of the plot in x', '10'),
                   'sites': ('differnet sites - input a python list with strings', '["gridka", "ekpcms6", "ekp-cloud", "ekpsg", "ekpsm","bwforcluster"]'),
                   'plotsize_y': ('size of plot in y', '5.8'),
                   'plot_width': ('width of bars in plot', '1'),
                   }

    table_columns = [
        Column('filename_plot', TEXT),
        Column('error_msg', TEXT)
    ], ['filename_plot']

    def prepareAcquisition(self):
        link = self.config['sourceurl']
        self.plotrange = int(self.config['plotrange'])
        self.plotsize_x = float(self.config['plotsize_x'])
        self.plotsize_y = float(self.config['plotsize_y'])
        self.plot_width = float(self.config['plot_width'])
        temp = self.config['sites']
        self.sites = ast.literal_eval(temp)
        # Download the file
        self.source = hf.downloadService.addDownload(link)
        # Get URL
        self.source_url = self.source.getSourceUrl()
        # Set up Container for subtable data
        self.statistics_db_value_list = []

    def extractData(self):
        import matplotlib.gridspec as gridspec
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.font_manager import FontProperties
        data = {}
        data['filename_plot'] = ""
        data['error_msg'] = "0"
        current_time = time.time()
        path = self.source.getTmpPath()
        # open file
        with open(path, 'r') as f:
            # fix the JSON-File, so the file is valid
            content = f.read()
            content_fixed = content.replace("}, }", "} }")
            content_fixed = content_fixed.replace("},\n  }", "} }")
            services = json.loads(content_fixed)
        job_id_list = list(services.keys())
        if len(job_id_list) == 0:  # stop script if no jobs in condor
            data['status'] = 0.5
            data['error_msg'] = "No jobs in the last " + str(self.plotrange) + " hours."
            return data
        # create lists with all neeeded values
        completion_date_list = list(int(services[id]['CompletionDate'])for id in job_id_list)
        start_date_list = list(services[id]['JobStartDate']for id in job_id_list)
        final_status_list = list(int(services[id]['JobStatus'])for id in job_id_list)
        last_status_list = []
        for id in job_id_list:
            try:
                last_status_list.append(int(services[id]['LastJobStatus']))
            except KeyError:
                last_status_list.append(0)
            except ValueError:
                last_status_list.append(0)
        final_status_date_list = list(
            int(services[id]['EnteredCurrentStatus'])for id in job_id_list)
        qdate_list = list(int(services[id]['QueueDate'])for id in job_id_list)
        total_jobs = len(job_id_list)
        starttime = current_time - (self.plotrange * 60 * 60)
        qdate_list = list(map(lambda x: x - starttime, qdate_list))
        qdate_list = list(map(lambda x: round(float(x) / (60 * 60), 0), qdate_list))
        final_status_date_list = list(map(lambda x: x - starttime, final_status_date_list))
        final_status_date_list = list(
            map(lambda x: round(float(x) / (60 * 60), 0), final_status_date_list))
        # define lists for plot
        plot_data_running = np.zeros(self.plotrange + 1)
        plot_data_idle = np.zeros(self.plotrange + 1)
        plot_data_queued = np.zeros(self.plotrange + 1)
        plot_data_finished = np.zeros(self.plotrange + 1)
        plot_data_removed = np.zeros(self.plotrange + 1)
        plot_data_hosts = np.zeros(len(self.sites))
        for i in xrange(total_jobs):
            try:
                host = services[job_id_list[i]]['HostName']
            except KeyError:
                host = ""
            # some jobs have no start_date_list value
            if start_date_list[i] != "undefined":
                start_date_list[i] = round(
                    float(int(start_date_list[i]) - starttime) / (60 * 60), 0)
            # running jobs have a completion_date_list value of zero
            if completion_date_list[i] > 0:
                completion_date_list[i] = round(
                    float(completion_date_list[i] - starttime) / (60 * 60), 0)
            for k in xrange(len(self.sites)):
                if self.sites[k] in host:
                    plot_data_hosts[k] += 1

        ########################################
        # create Lists for barPlot, sorted by time
        ########################################
        # Function for handling of the time in queue, same for every job #
        def qtime_handling(qdate, startdate, plot_data_queued):
            k = int(round(qdate, 0))
            # if time when job was queued and starting time are older than plotrange,
            # set k to zero and detect job as running
            if k <= 0 and startdate <= 0:
                k = 0
            # if queue is older than plotrange but start is not - count job as queued
            # if k is zero or greater
            elif k < 0 and startdate > 0:
                while k < startdate:
                    if k >= 0:
                        plot_data_queued[k] += 1
                    k += 1
            # queued in plotrange - just check time queued
            else:
                while k <= startdate:
                    plot_data_queued[k] += 1
                    k += 1
            return k
    # Function to set the lists for the last two states correctly

        def final_handling(completion, k, final_state_list, last_state_list):
            # while k is smaller than the time entering the final state, put the job
            # in the last state before that and iterate
            while k <= completion:
                last_state_list[k] += 1
                k += 1
            # if the arravial in the final state is less than an hour ago - put job in
            # final_state and last_state at the same time
            if k == self.plotrange + 1:
                final_state_list[k - 1] += 1
            # otherwise - put job in final state in the next hour
            else:
                final_state_list[k] += 1

        for i in xrange(total_jobs):
            # exclude jobs that are still running for the moment
            if completion_date_list[i] != 0:
                # normal jobs, finished with completed and before they ran
                if final_status_list[i] == 4 and last_status_list[i] == 2:
                    k = qtime_handling(qdate_list[i], start_date_list[i], plot_data_queued)
                    final_handling(completion_date_list[i], k,
                                   plot_data_finished, plot_data_running)
                # jobs that completed but were removed after that
                elif final_status_list[i] == 3 and last_status_list[i] == 4:
                    k = qtime_handling(qdate_list[i], start_date_list[i], plot_data_queued)
                    final_handling(completion_date_list[i], k, plot_data_removed, plot_data_running)
                # jobs that were running and then went on hold
                elif final_status_list[i] == 5 and last_status_list[i] == 2:
                    k = qtime_handling(qdate_list[i], start_date_list[i], plot_data_queued)
                    final_handling(completion_date_list[i], k,
                                   plot_data_finished, plot_data_running)
                # jobs that were idle and then finished
                elif final_status_list[i] == 4 and last_status_list[i] == 1:
                    k = qtime_handling(qdate_list[i], start_date_list[i], plot_data_queued)
                    final_handling(completion_date_list[i], k, plot_data_finished, plot_data_queued)
        # jobs that dont have an CompletionDate
            else:
                # jobs that started, queued and finished plotrange hours ago
                if final_status_list[i] == 4 and last_status_list[i] == 2:
                    if qdate_list[i] == 0 and start_date_list[i] == 0 and completion_date_list[i] == 0:
                        plot_data_finished[0] += 1
        # jobs that were removed and were on hold before - never ran
                elif final_status_list[i] == 3 and last_status_list[i] == 5:
                    k = qtime_handling(qdate_list[i], start_date_list[i], plot_data_queued)
                    if final_status_date_list[i] >= 0 and k > 25:
                        plot_data_removed[k] += 1
                    elif k == 25:
                        plot_data_removed[24] += 1
        # jobs that were removed and ran before
                elif final_status_list[i] == 3 and last_status_list[i] == 2:
                    k = qtime_handling(qdate_list[i], start_date_list[i], plot_data_queued)
                    final_handling(final_status_date_list[i],
                                   k, plot_data_removed, plot_data_running)
        # jobs that were removed and before removed
                elif final_status_list[i] == 3 and last_status_list[i] == 3:
                    k = qtime_handling(qdate_list[i], start_date_list[i], plot_data_queued)
                    final_handling(final_status_date_list[i],
                                   k, plot_data_removed, plot_data_queued)
        # jobs from condor_q that are still running
                elif final_status_list[i] == 2 and last_status_list[i] == 1:
                    k = qtime_handling(qdate_list[i], start_date_list[i], plot_data_queued)
                    if k == self.plotrange + 1:
                        plot_data_running[k - 1] += 1
                    else:
                        k + 1
                    while k < self.plotrange + 1:
                        plot_data_running[k] += 1
                        k += 1
        # jobs that are idle at the moment in condor_q and havent started yet
                elif final_status_list[i] == 1 and last_status_list[i] == 0:
                    k = round(qdate_list[i])
                    if k < 0:
                        k = 0
                    while k < self.plotrange + 1:
                        plot_data_queued[k] += 1
                        k += 1
        # queued Jobs that got removed before they started to run
                elif final_status_list[i] == 3 and last_status_list[i] == 1:
                    k = qtime_handling(qdate_list[i], start_date_list[i], plot_data_queued)
                    k = round(final_status_date_list[i])
                    if k >= 0:
                        plot_data_removed[k] += 1
        # jobs that completed - so ran before, but then got removed
                elif final_status_list[i] == 3 and last_status_list[i] == 4:
                    k = qtime_handling(qdate_list[i], start_date_list[i], plot_data_queued)
                    final_handling(final_status_date_list[i],
                                   k, plot_data_removed, plot_data_running)
        # job that went from running to idle
                elif final_status_list[i] == 1 and last_status_list[i] == 2:
                    k = qtime_handling(qdate_list[i], start_date_list[i], plot_data_queued)
                    final_handling(final_status_date_list[i], k, plot_data_idle, plot_data_running)
                    k += 1
                    while k < self.plotrange + 1:
                        plot_data_idle[k] += 1
                        k += 1
        # job that went from running to removed
                elif final_status_list[i] == 5 and last_status_list[i] == 2:
                    k = qtime_handling(qdate_list[i], start_date_list[i], plot_data_queued)
                    final_handling(final_status_date_list[i], k, plot_data_removed, plot_data_running)

        ###############
        # Make   plot #
        ###############
        plot_color = {
            'removed' : '#e69f00',
            'running' : '#d55e00',
            'finished': '#009e73',
            'queued'  : '#0072b2',
            'idle'    : '#56b4e9',
        }
        # define size according to config
        fig = plt.figure(figsize=(self.plotsize_x, self.plotsize_y*2))
        gs = gridspec.GridSpec(2, 1)
        axis = plt.subplot(gs[0, 0])
        axis_2 = plt.subplot(gs[1, 0])
        ind = np.arange(self.plotrange + 1)
        ind_2 = np.arange(len(self.sites))
        width = self.plot_width
        bar_1 = axis.bar(ind, plot_data_running, width, color=plot_color['running'], align='center')
        bar_2 = axis.bar(ind, plot_data_queued, width, bottom=plot_data_running,
                         color=plot_color['queued'], align='center')
        bar_3 = axis.bar(ind, plot_data_idle, width, bottom=plot_data_running +
                         plot_data_queued, color=plot_color['idle'], align='center')
        bar_4 = axis.bar(ind, plot_data_removed, width, bottom=plot_data_queued +
                         plot_data_running + plot_data_idle, color=plot_color['removed'], align='center')
        bar_5 = axis.bar(ind, plot_data_finished, width, bottom=plot_data_queued + plot_data_running +
                         plot_data_removed + plot_data_idle, color=plot_color['finished'], align='center')
        axis.set_xlabel("Time")
        axis.set_ylabel("Jobs")
        xlabels = []
        for i in xrange(self.plotrange + 1):
            if i % 2 == 0:
                time_tick = starttime + i * 60 * 60
                xlabels.append(datetime.datetime.fromtimestamp(
                    float(time_tick)).strftime('%d.%m \n %H:%M'))
            else:
                xlabels.append("")
        today_readable = datetime.datetime.fromtimestamp(
            float(current_time)).strftime('%Y-%m-%d %H:%M:%S')
        starttime_readable = datetime.datetime.fromtimestamp(
            float(starttime)).strftime('%Y-%m-%d %H:%M:%S')
        axis.set_title("Job Distribution from " + str(starttime_readable) +
                       " CET to " + str(today_readable) + " CET")
        axis.set_xticks(ind)
        axis.set_xticklabels(xlabels, rotation='vertical')
        axis.set_xlim(-1, self.plotrange + 1)
        fontLeg = FontProperties()
        fontLeg.set_size('small')
        axis.legend((bar_1[0], bar_2[0], bar_3[0], bar_4[0], bar_5[0]),
                    ("runnings jobs", "queued jobs", "idle jobs", "removed jobs", "finished jobs"),
                    loc=6, bbox_to_anchor=(0.8, 0.88), borderaxespad=0., prop = fontLeg)
        # plot that shows site usage
        bar_21 = axis_2.bar(ind_2, plot_data_hosts, width*0.5, align='center', color=plot_color['finished'])
        for rect in bar_21:
            height = rect.get_height()
            axis_2.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                        '%d' % int(height),
                        ha='center', va='bottom')
        axis_2.set_xticks(ind_2)
        max_height = axis_2.get_ylim()[1]
        axis_2.set_ylim(0, max_height*1.2)
        axis_2.set_xlim(-0.5, len(self.sites)-0.5)
        axis_2.set_xticklabels(self.sites)
        axis_2.set_title("Site Distribution from " + str(starttime_readable) +
                         " CET to " + str(today_readable) + " CET")
        axis_2.set_ylabel("finished Jobs")
        axis_2.set_xlabel("Sites")
        plt.tight_layout()
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + "_history.png"), dpi=91)
        data["filename_plot"] = self.instance_name + "_history.png"
        print data
        return data
