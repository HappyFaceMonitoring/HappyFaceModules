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
from operator import add
import time
import ast


class V2CMS6Status(hf.module.ModuleBase):
    ''' define condig values to be used later'''
    config_keys = {'sourceurl': ('Source Url', ''),
                   'plotsize_x': ('Size of the plot in x', '10'),
                   'plotsize_y': ('Size of the plot in y', '5.8'),
                   'plot_width': ('width of the bars in plot', '0.6'),
                   'plot_right_margin': ('white space between biggest bar and right side of the plot in %', '0.1'),
                   'min_plotsize': ('how much bars the plots shows at least before scaling bigger', '3'),
                   'log_limit': ('x-Value, when to use a log scale', '500'),
                   'jobs_min': ('min jobs to determine status', '50'),
                   'qtime_max': ('maximal queue time for jobs', '24'),
                   'qtime_max_jobs': ('max how many jobs can have a longer qtime', '20'),
                   'running_idle_ratio': ('min ratio between running and idle jobs', '0.2'),
                   'min_efficiency': ('min efficiency', '0.8'),
                   'sites': ('differnet sites - input a python list with strings', '["gridka", "ekpcms6", "ekp-cloud", "ekpsg", "ekpsm","bwforcluster"]')
                   }

    table_columns = [
        Column('running_jobs', INT),
        Column('queued_jobs', INT),
        Column('cores', INT),
        Column('ram', INT),
        Column('efficiency', INT),
        Column('qtime', TEXT),
        Column('error', INT),
        Column('error_msg', TEXT),
        Column('filename_plot', TEXT)
    ], ['filename_plot']

    subtable_columns = {
        'statistics': ([
            Column("user", TEXT),
            Column("idle", TEXT),
            Column("queued", TEXT),
            Column("running", TEXT),
            Column("removed", TEXT),
            Column("finished", TEXT),
            Column("held", INT),
            Column("suspended", INT),
            Column("core", TEXT),
            Column("efficiency", TEXT),
            Column("sites", TEXT),
            Column("ram", TEXT)], []),

        'jobs': ([
            Column("status", INT),
            Column("jobid", TEXT),
            Column("cpu", TEXT),
            Column("user", TEXT),
            Column("host", TEXT),
            Column("ram", TEXT)], [])
    }

    def prepareAcquisition(self):
        link = self.config['sourceurl']
        self.plotsize_x = float(self.config['plotsize_x'])
        self.plotsize_y = float(self.config['plotsize_y'])
        self.plot_width = float(self.config['plot_width'])
        self.log_limit = int(self.config['log_limit'])
        self.min_plotsize = float(self.config['min_plotsize'])
        self.plot_right_margin = float(self.config['plot_right_margin'])
        self.jobs_min = int(self.config['jobs_min'])
        self.qtime_max = int(self.config['qtime_max'])
        self.running_idle_ratio = float(self.config['running_idle_ratio'])
        self.min_efficiency = float(self.config['min_efficiency'])
        self.qtime_max_jobs = int(self.config['qtime_max_jobs'])
        temp = self.config['sites']
        self.sites = ast.literal_eval(temp) # fix fromat so sites is a list of strings
        self.source = hf.downloadService.addDownload(link)  # Download the file
        self.source_url = self.source.getSourceUrl()  # Get URL
        # Set up Container for subtable data
        self.statistics_db_value_list = []
        self.jobs_db_value_list = []

    def extractData(self):
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.font_manager import FontProperties
        #  define default values
        data = {}
        details_data = {}
        data["filename_plot"] = ''
        data["error_msg"] = ''
        data['error'] = 0
        path = self.source.getTmpPath()
        ''' representation of the different states in condor:
            Value 	Status
            1   	Idle
            2   	Running
            3   	Removed (not plotted)
            4   	Completed (not plotted)
            5   	Held
            6   	Transferring Output (not plotted)
            7   	Suspended
            '''
        status_codes = [2, 3, 4, 5, 6, 7]
        # Function to convert seconds to readable time format

        def seconds_to_time(seconds):
            try:
                m, s = divmod(float(seconds), 60)
                h, m = divmod(m, 60)
                return "%d:%02d:%02d" % (h, m, s)
            except ValueError:
                return "Undefined"

        # open file
        with open(path, 'r') as f:
            content = f.read()
        if '{ }' in content:  # if no jobs in condor_q, stop script and display error_msg inst.
            data['status'] = 1
            data['error'] = 1
            data['error_msg'] = "No Jobs running"
            return data
        content_fixed = content.replace("}, }", "} }")  # fix JSON to be valid
        services = json.loads(content_fixed)  # load JSON
        job_id_list = list(services.keys())  # list of jobs
        # create lists with all neeeded values
        status_list = list(int(services[id]['Status'])for id in job_id_list)
        ram_list = list(services[id]['RAM']for id in job_id_list)
        cpu1_list = list(services[id]['Cpu_1']for id in job_id_list)
        cpu2_list = list(services[id]['Cpu_2']for id in job_id_list)
        core_list = list(int(services[id]["RequestedCPUs"])for id in job_id_list)
        user_list = list(services[id]['User'] for id in job_id_list)
        qdate_list = list(services[id]['QueueDate'] for id in job_id_list)
        host_list = list(services[id]['HostName'] for id in job_id_list)
        jobstart_list = list(services[id]['JobStartDate']for id in job_id_list)
        last_status_list = []
        for id in job_id_list:
            try:
                last_status_list.append(int(services[id]['LastJobStatus']))
            except KeyError:
                last_status_list.append(0)

        # calculate runtime from cpu_1 and cpu_2
        cpu_time_list = map(add, map(float, cpu1_list), map(float, cpu2_list))  #total cpu_time is cpu1+cpu2
        job_count = len(job_id_list)
        plot_names = list(set(user_list))  # Create Array with unique Usernames
        # initiate arrays for plot and subtable
        plot_status = {
            "queued": np.zeros(len(plot_names)),
            "idle": np.zeros(len(plot_names)),
            2: np.zeros(len(plot_names)),
            3: np.zeros(len(plot_names)),
            4: np.zeros(len(plot_names)),
            5: np.zeros(len(plot_names)),
            6: np.zeros(len(plot_names)),
            7: np.zeros(len(plot_names)),
        }
        plot_cores = np.zeros(len(plot_names))
        plot_ram = np.zeros(len(plot_names))
        plot_efficiency = np.zeros(len(plot_names))
        plot_efficiency_count = np.zeros(len(plot_names))
        plot_sites = [''] * len(plot_names)
        efficiency_list = []
        qtime_list = []
        sites = self.sites
        current_time = time.time()
        ###################
        #   Calculations  #
        ###################
        # Calculation of the efficiency and time in Queue per job
        for i in xrange(job_count):
            # calculate how much cores are used by running jobs
            if status_list[i] != 2:
                core_list[i] = 0
            # calculate the efficiency per job
            try:
                efficiency_list.append(
                    round(float(cpu_time_list[i]) / float(current_time - int(jobstart_list[i])), 3))
            except (ZeroDivisionError, ValueError):
                efficiency_list.append("Undefined")
            # calculate the qtime per job
            try:
                qtime_list.append(int(jobstart_list[i]) - int(qdate_list[i]))
            except ValueError:
                qtime_list.append("Undefined")
        for k in xrange(len(host_list)):  # shorten host_list to site name only
            if host_list[k] != "undefined":
                for i in xrange(len(sites)):
                    if sites[i] in host_list[k]:
                        host_list[k] = sites[i]
        for i in xrange(job_count):
            try:  # fix RAM - List so only values
                ram_list[i] = round(float(ram_list[i]) / (1024 * 1024), 2)
            except ValueError:
                pass
            # generate the array used in plot later to show data per user
            for k in xrange(len(plot_names)):  # sort jobs via user to get data for plot
                if user_list[i] == plot_names[k]:
                    for status in status_codes:
                        if status_list[i] == status:
                            plot_status[status][k] += 1
                    if status_list[i] == 1 and last_status_list[i] == 2:
                        plot_status["idle"][k] += 1
                    elif status_list[i] == 1 and last_status_list[i] == 0:
                        plot_status["queued"][k] += 1
                    plot_cores[k] += core_list[i]  # cores per user
                    if ram_list[i] != 'undefined':  # ram per user
                        plot_ram[k] += ram_list[i]
                    if efficiency_list[i] != 'Undefined':  # eff per user
                        plot_efficiency[k] += efficiency_list[i]
                        plot_efficiency_count[k] += 1
                    if host_list[i] not in plot_sites[k] and host_list[i] != 'undefined':
                        if plot_sites[k] == '':  # sites used per user
                            plot_sites[k] = host_list[i]
                        else:
                            plot_sites[k] += ", " + host_list[i]
            for k in xrange(len(plot_names)):
                if plot_sites[k] == "":
                    plot_sites[k] = "Undefined"
        # fill subtable statistics
        for i in xrange(len(plot_names)):
            if plot_efficiency_count[i] == 0.0:  # Calculation of efficiency
                eff = 0.0
            else:
                try:
                    eff = round(float(plot_efficiency[i]) / plot_efficiency_count[i], 2)
                except (ZeroDivisionError, ValueError):
                    eff = 0.0
            details_data = {
                'user':      plot_names[i],
                'queued':    int(plot_status["queued"][i]),
                'idle':      int(plot_status["idle"][i]),
                'running':   int(plot_status[2][i]),
                'removed':   int(plot_status[3][i]),
                'finished':  int(plot_status[4][i]),
                'held':      int(plot_status[5][i]),
                'suspended': int(plot_status[7][i]),
                'core':      int(plot_cores[i]),
                'sites':     plot_sites[i],
                'ram':       round(plot_ram[i], 1),
                'efficiency': eff}
            self.statistics_db_value_list.append(details_data)
        # fill subtable jobs
        for i in xrange(job_count):
            details_data = {
                'jobid':  job_id_list[i],
                'cpu':    seconds_to_time(cpu_time_list[i]),
                'ram':    ram_list[i],
                'status': status_list[i],
                'user':   user_list[i],
                'host':   host_list[i]}
            self.jobs_db_value_list.append(details_data)

        ###############
        # Make   plot #
        ###############
        # A Plot that shows Jobs per User and status of the jobs
        plot_color = {  # define colors to be used
            # 'queued':     '#44a4df',
            # 'idle':       '#14e2e7',
            # 'running':    '#59ff8e',
            # 'held':       '#a7f06e',
            # 'suspended':  '#cbf064',
            'suspended':  '#CFF09E',
            'held':       '#A8DBA8',
            'running':    '#79BD9A',
            'queued':     '#3B8686',
            'idle':       '#0B486B',
        }

        if len(plot_names) <= self.min_plotsize:  # set plot size depending on how much users are working
            y = self.plotsize_y
        else:
            y = round((self.plotsize_y / self.min_plotsize), 1) * len(plot_names)
        fig = plt.figure(figsize=(self.plotsize_x, y))
        axis = fig.add_subplot(111)
        ind = np.arange(len(plot_names))
        width = self.plot_width
        # create stacked horizontal bars
        bar_1 = axis.barh(
            ind, plot_status["queued"], width, color=plot_color['queued'], align='center')
        bar_2 = axis.barh(
            ind, plot_status[2], width, color=plot_color['running'], align='center',
            left=plot_status["queued"])
        bar_3 = axis.barh(
            ind, plot_status["idle"], width, color=plot_color['idle'], align='center',
            left=plot_status["queued"] + plot_status[2])
        bar_4 = axis.barh(
            ind, plot_status[5], width, color=plot_color['held'], align='center',
            left=plot_status[2] + plot_status["idle"] + plot_status["queued"])
        bar_5 = axis.barh(
            ind, plot_status[7], width, color=plot_color['suspended'], align='center',
            left=plot_status[5] + plot_status[2] + plot_status["idle"] + plot_status["queued"])
        max_width = axis.get_xlim()[1]
        if max_width <= 10:  # set xlim for 10 jobs or less
            axis.set_xlim(0, 10)
        # use log scale if max_width gets bigger than 1000
        if max_width >= self.log_limit and len(plot_names) >= 3:
            print len(plot_names)
            bar_1 = axis.barh(
                ind, plot_status["queued"], width, color=plot_color['queued'], align='center', log=True)
            bar_2 = axis.barh(
                ind, plot_status[2], width, color=plot_color['running'], align='center', log=True,
                left=plot_status["queued"])
            bar_3 = axis.barh(
                ind, plot_status["idle"], width, color=plot_color['idle'], align='center',
                left=plot_status["idle"] + plot_status["2"], log=True)
            bar_4 = axis.barh(
                ind, plot_status[5], width, color=plot_color['held'], align='center',
                left=plot_status[2] + plot_status["idle"] + plot_status["queued"], log=True)
            bar_5 = axis.barh(
                ind, plot_status[7], width, color=plot_color['suspended'], align='center',
                left=plot_status[5] + plot_status[2] + plot_status["idle"] + plot_status["queued"], log=True)
            for i in xrange(len(plot_names)):  # position name tags for log plot
                temp = plot_names[i] + " - " + str(int(plot_status["idle"][i] + plot_status[7][i] + plot_status[2][
                                                    i] + plot_status[5][i] + plot_status["queued"][i])) + " Jobs"
                axis.text(axis.get_xlim()[0] + 0.5, i + 0.37, temp, ha='left', va="center")
        else:  # position name tags for normal plot
            for i in xrange(len(plot_names)):
                temp = plot_names[i] + " - " + str(int(plot_status["idle"][i] + plot_status[7][i] + plot_status[2][
                                                   i] + plot_status[5][i] + plot_status["queued"][i])) + " Jobs"
                axis.text(1, i + 0.37, temp, ha='left', va="center")
        if len(plot_names) < self.min_plotsize:   # set ylimit so fix look of plots with few users
            axis.set_ylim(-0.5, self.min_plotsize - 0.5)
        else:
            axis.set_ylim(-0.5, len(plot_names) - 0.5)
        # cuten plot
        max_width = int(axis.get_xlim()[1] * (self.plot_right_margin + 1))
        min_width = axis.get_xlim()[0]
        axis.set_xlim(min_width, max_width)
        axis.set_title('jobs per user')
        axis.set_xlabel('number of jobs')
        axis.set_ylabel('user')
        axis.set_yticks(ind)
        axis.set_yticklabels('')
        fontLeg = FontProperties()
        fontLeg.set_size('small')
        axis.legend((bar_1[0], bar_2[0], bar_3[0], bar_4[0], bar_5[0]), (
            'queued jobs', 'running jobs', 'idle jobs', 'held jobs', 'suspended jobs'),
            loc=6, bbox_to_anchor=(0.8, 0.88), borderaxespad=0., prop = fontLeg)
        plt.tight_layout()
        # save data as Output
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + "_jobs2.png"), dpi=91)
        data["filename_plot"] = self.instance_name + "_jobs2.png"
        # remove Undefined efficiencys from list to calc mean efficiency
        while "Undefined" in efficiency_list:
            efficiency_list.remove("Undefined")
        try:
            eff_count = round(float(sum(efficiency_list) / len(efficiency_list)), 2)
        except ZeroDivisionError:
            eff_count = 0.0
        data['efficiency'] = eff_count
        # fix RAM - List so only values
        for ram in ram_list:
            try:
                ram = float(ram)
            except ValueError:
                ram = 0
        data['ram'] = round(sum(ram_list), 2)
        while "Undefined" in qtime_list:  # filter undefined qtime values from qtime list
            qtime_list.remove("Undefined")
        try:
            qtime_count = int(round(float(sum(qtime_list)) / len(qtime_list), 0))
        except ZeroDivisionError:
            qtime_count = 0
        except ValueError:
            qtime_count = 0
        data['qtime'] = seconds_to_time(qtime_count)
        data['cores'] = sum(core_list)
        data['running_jobs'] = sum(plot_status[2])
        data['queued_jobs'] = sum(plot_status["queued"])
        #########################
        # calculation of Status #
        #########################
        if job_count < self.jobs_min:
            data['status'] = 0.5
            data['error_msg'] = "Only " + str(job_count) + " jobs running."
        else:
            temp = 0
            for i in xrange(job_count):
                if status_list[i] == 1 and int(current_time) - int(qdate_list[i]) > (self.qtime_max*60*60):
                    temp += 1
            if temp > self.qtime_max_jobs:
                data['status'] = 0.5
                data['error_msg'] = data['error_msg'] + \
                    str(temp) + " jobs are longer than " + \
                    str(self.qtime_max) + " hours in the queue. <br>"
            if eff_count < self.min_efficiency and data['running_jobs'] != 0:
                data['status'] = 0
                data['error_msg'] = data['error_msg'] + \
                    " The efficiency is below " + str(self.min_efficiency) + ".<br>"
            try:
                if float(status_list.count(2)) / float(status_list.count(1)) < self.running_idle_ratio:
                    data['status'] = 0
                    data['error_msg'] = data['error_msg'] + \
                        "The ratio between queued and running jobs is below " + \
                        str(self.running_idle_ratio) + ". <br>"
            except ZeroDivisionError:
                pass
        print data
        return data

    # Putting Data in the Subtable to display
    def fillSubtables(self, parent_id):
        self.subtables['statistics'].insert().execute(
            [dict(parent_id=parent_id, **row) for row in self.statistics_db_value_list])
        self.subtables['jobs'].insert().execute(
            [dict(parent_id=parent_id, **row) for row in self.jobs_db_value_list])

    # Making Subtable Data available to the html-output
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['statistics'].select().where(
            self.subtables['statistics'].c.parent_id == self.dataset['id']
        ).execute().fetchall()
        data["statistics"] = map(dict, details_list)

        details_list = self.subtables['jobs'].select().where(
            self.subtables['jobs'].c.parent_id == self.dataset['id']
        ).execute().fetchall()
        data["jobs"] = map(dict, details_list)

        return data
