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
import ast


class CMS6MachineStatus(hf.module.ModuleBase):
    config_keys = {'sourceurl': ('Source Url', ''),
                   'plotsize_x': ('Size of the plot in x', '10'),
                   'plotsize_y': ('Size of the plot in y', '3.6'),
                   'plot_width': ('width of the bars in plot', '0.38'),
                   'log_limit': ('x-Value, when to use a log scale', '500'),
                   'plot_right_margin': ('white space between biggest bar and right side of the plot in %', '0.1'),
                   'weak_threshold': ('Weak Slots have a load below this value', '0.5'),
                   'min_plotsize': ('how much bars the plots shows at least before scaling bigger', '3'),
                   'sites': ('differnet sites - input a python list with strings', '["gridka", "ekpcms6", "ekp-cloud", "ekpsg", "ekpsm","bwforcluster"]'),
                   'machine_slot_min': ('min many slots per machine', '2'),
                   'claimed_unclaimed_ratio': ('min limit for claimed_unclaimed_ratio', '0.3'),
                   'weak_slots_limit': ('max weak slots in %', '0.1'),
                   'slots_min': ('min slots to determine status', '20'),
                   'weak_threshold': ('weak Slots have a avgload below this value', '0.5')
                   }
    table_columns = [
        Column('claimedslots_loadavg', INT),
        Column('unclaimedslots_loadavg', INT),
        Column('weak_slots', INT),
        Column('machines', INT),
        Column('slots', INT),
        Column('claimed_slots', FLOAT),
        Column('filename_plot', TEXT),
        Column('error', INT),
        Column('error_msg', TEXT),
        Column('condor_load', FLOAT),
        Column('unclaimed_slots', FLOAT),
    ], ['filename_plot']

    subtable_columns = {
        'statistics': ([
            Column('mid', TEXT),
            Column('status', TEXT),
            Column('activity', TEXT),
            Column('LoadAvg', FLOAT)
        ], []),

        'condor': ([
            Column('site', TEXT),
            Column('condor_version', TEXT),
            Column('value', TEXT),
        ], []),

        'plot': ([
            Column('site', TEXT, index = True),
            Column('claimed', INT),
            Column('unclaimed', INT),
            Column('machines', INT),
            Column('idle', INT),
            Column('busy', INT),
            Column('suspended', INT),
            Column('retiring', INT),
            Column('claimed_avg', FLOAT),
            Column('disk', FLOAT),
            Column('unclaimed_avg', FLOAT)
        ], [])
    }

    def prepareAcquisition(self):
        ''' acquire the config parameters given in the config file'''
        link = self.config['sourceurl']
        self.plotsize_x = float(self.config['plotsize_x'])
        self.plotsize_y = float(self.config['plotsize_y'])
        self.plot_width = float(self.config['plot_width'])
        self.log_limit = int(self.config['log_limit'])
        self.min_plotsize = float(self.config['min_plotsize'])
        self.plot_right_margin = float(self.config['plot_right_margin'])
        self.weak_threshold = float(self.config['weak_threshold'])
        self.machine_slot_min = float(self.config['machine_slot_min'])
        self.claimed_unclaimed_ratio = float(self.config['claimed_unclaimed_ratio'])
        self.slots_min = int(self.config['slots_min'])
        self.weak_slots_limit = float(self.config['weak_slots_limit'])
        temp = self.config['sites']
        self.sites = ast.literal_eval(temp)
        # Download the file
        self.source = hf.downloadService.addDownload(link)
        # Get URL
        self.source_url = self.source.getSourceUrl()
        # Set up Container for subtable data
        self.statistics_db_value_list = []
        self.plot_db_value_list = []
        self.condor_db_value_list = []

    def extractData(self):
        import numpy as np
        import matplotlib.pyplot as plt
        from matplotlib.font_manager import FontProperties

        def sitescan(machine_name, sites):  # function to find site for machine_name
            i = 0
            while sites[i] not in machine_name and i + 1 < len(sites):
                i += 1
            return sites[i]
        data = {}
        data["filename_plot"] = ''
        data["error_msg"] = ''
        data['unclaimedslots_loadavg'] = 0
        data['claimedslots_loadavg'] = 0
        ''' list of possible acitvities of a slot
            "Idle":There is no job activity
            "Busy":A job is busy running
            "Suspended":A job is currently suspended
            "Retiring":Waiting for a job to finish or for the maximum retirement time to expire
        '''
        act_states = ["Idle", "Busy", "Suspended", "Retiring"]
        sites = self.sites  # list of all available sites
        details_data = {}
        path = self.source.getTmpPath()
        # open file
        with open(path, 'r') as f:
            # fix the JSON-File, so the file is valid
            content = f.read()
            if '{ }' in content:  # if no jobs in condor_q, stop script and display error_msg inst.
                data['status'] = 1
                data['error'] = 1
                data['error_msg'] = "No Slots running"
                return data
            content_fixed = content.replace("}, }", "} }")
            services = json.loads(content_fixed)
            slot_id_list = list(services.keys())
        # create lists with the values of 'State' and 'Activity' to count them
        state_list = list(services[id]['State']for id in slot_id_list)
        activity_list = list(services[id]['Activity'] for id in slot_id_list)
        load_list = list(float(services[id]['LoadAvg']) for id in slot_id_list)
        machine_name_list = list(services[id]['Machine']for id in slot_id_list)
        condor_version_list = list(services[id]['CondorVersion']for id in slot_id_list)
        condor_load_list = list(float(services[id]['TotalCondorLoadAvg'])for id in slot_id_list)
        disk_list = list(round(float(services[id]['Disk'])/(1024*1024), 2)for id in slot_id_list)
        total_slot_list = list(int(services[id]['TotalSlots'])for id in slot_id_list)
        slot_count = len(slot_id_list)
        condor_version_list = list(condor_version_list[i].replace(
            "$", "")for i in xrange(slot_count))
        machine_names = list(set(machine_name_list))  # list of different machines
        condor_versions = list(set(condor_version_list))  # get list of different condor_versions
        machine_slots = [0] * len(machine_names)
        for i in xrange(slot_count):  # calculate how much slots are online in total
            for k in xrange(len(machine_names)):
                if machine_name_list[i] == machine_names[k] and machine_slots[k] == 0:
                    machine_slots[k] = total_slot_list[i]
        for k in xrange(len(machine_names)):  # get machine_names reduced to name of different sites
            machine_names[k] = sitescan(machine_names[k], sites)
        # create Arrays for plot and additional ones for Plot Details Subtable:
        plot_claimed = np.zeros(len(sites))
        plot_unclaimed = np.zeros(len(sites))
        plot_machines_per_site = np.zeros(len(sites))
        plot_avg_load_claimed = np.zeros(len(sites))
        plot_avg_load_unclaimed = np.zeros(len(sites))
        plot_disk = np.zeros(len(sites))
        plot_weak = np.zeros(len(sites))
        # lists for different possilble activities
        plot_activity = {
            "Idle": np.zeros(len(sites)),
            "Busy": np.zeros(len(sites)),
            "Suspended": np.zeros(len(sites)),
            "Retiring": np.zeros(len(sites))
        }
        condor_version_per_site = {}

        '''Checking how many machines and how many slots are available per site and fill lists to
         shorten the information '''
        for i in xrange(len(machine_name_list)):
            for j in xrange(len(sites)):
                if sites[j] in machine_name_list[i]:  # how much machines are running per site
                    plot_disk[j] += disk_list[i]
                    if state_list[i] == "Claimed":  # how much slots are claimed or set on Owner -> not available for new jobs
                        plot_claimed[j] += 1
                        plot_avg_load_claimed[j] += load_list[i]
                        if load_list[i] <= self.weak_threshold:
                            plot_weak[j] += 1
                    elif activity_list[i] == "Idle" or state_list[i] == "Owner":  # how much slots are idle
                        plot_unclaimed[j] += 1
                        plot_avg_load_unclaimed[j] += load_list[i]
                    # filter by activity for the plot since activity is more interesting
                    for activity in act_states:
                        if activity_list[i] == activity:
                            plot_activity[activity][j] += 1
        for j in xrange(len(sites)):  # calculate the average load per site
            plot_machines_per_site[j] = machine_names.count(sites[j])
            try:
                plot_avg_load_claimed[j] = round(
                    float(plot_avg_load_claimed[j]) / float(plot_claimed[j]), 2)
            except (ValueError, ZeroDivisionError):
                plot_avg_load_claimed[j] = 0
            try:
                plot_avg_load_unclaimed[j] = round(
                    float(plot_avg_load_unclaimed[j]) / float(plot_unclaimed[j]), 2)
            except (ValueError, ZeroDivisionError):
                plot_avg_load_unclaimed[j] = 0
        for j in xrange(len(sites)):  # count different condor versions
            temp = {}
            for k in xrange(len(condor_versions)):  # check different condor_versions per site
                temp[condor_versions[k]] = 0
            condor_version_per_site[sites[j]] = temp
        for i in xrange(slot_count):
            condor_version_per_site[sitescan(machine_name_list[i], sites)][
                condor_version_list[i]] += 1
        ###############
        # Make   plot #
        ###############
        plot_color = {
            # 'queued':   '#5CADFF',
            # 'idle':     '#9D5CDE',
            # 'running':  '#85CE9D',
            # 'finished': '#009933',
            # 'removed':  '#CC6060',
            'suspended':  '#CFF09E',
            'busy':    '#79BD9A',
            'retiring':     '#3B8686',
            'idle':       '#0B486B',
        }
        # set plot size according to config and data size
        if len(sites) <= self.min_plotsize:
            y = self.plotsize_y
        else:
            y = round(self.plotsize_y / self.min_plotsize, 1) * len(sites)
        fig = plt.figure(figsize=(self.plotsize_x, y))
        axis = fig.add_subplot(111)
        ind = np.arange(len(sites))
        width = self.plot_width
        # create stacked horizontal bars
        bar_1 = axis.barh(ind, plot_activity["Idle"], width, color=plot_color['idle'], align='center')
        bar_2 = axis.barh(ind, plot_activity["Busy"], width, color=plot_color[
                          'busy'], align='center', left=plot_activity["Idle"])
        bar_3 = axis.barh(ind, plot_activity["Suspended"], width, color=plot_color[
                          'suspended'], align='center', left=plot_activity["Idle"] + plot_activity["Busy"])
        bar_4 = axis.barh(ind, plot_activity["Retiring"], width, color=plot_color[
                          'retiring'], align='center', left=plot_activity["Idle"] + plot_activity["Busy"] + plot_activity["Suspended"])
        max_width = axis.get_xlim()[1]
        # use log scale if max_width gets bigger than 1000
        if max_width >= self.log_limit:
            bar_1 = axis.barh(ind, plot_activity["Idle"], width, color=plot_color['idle'], align='center', log=True)
            bar_2 = axis.barh(ind, plot_activity["Busy"], width, color=plot_color[
                              'busy'], align='center', left=plot_activity["Idle"], log=True)
            bar_3 = axis.barh(ind, plot_activity["Suspended"], width, color=plot_color[
                              'suspended'], align='center', left=plot_activity["queued"] + plot_activity["Busy"], log=True)
            bar_4 = axis.barh(ind, plot_activity["Retiring"], width, color=plot_color[
                              'retiring'], align='center', left=plot_activity["finished"] + plot_activity["Busy"] + plot_activity["Suspended"], log=True)
            for i in xrange(len(sites)):
                temp = sites[i] + " - " + \
                    str(int(plot_unclaimed[i] + plot_claimed[i])) + " Slots"
                axis.text(axis.get_xlim()[0] + 0.5, i + (width / 2) +
                          0.07, temp, ha='left', va="center")
        else:
            for i in xrange(len(sites)):
                temp = sites[i] + " - " + \
                    str(int(plot_unclaimed[i] + plot_claimed[i])) + " Slots"
                axis.text(1, i + (width / 2) + 0.07, temp, ha='left', va="center")
        # set ylimit so fix look of plots with few users
        if len(sites) < self.min_plotsize:
            axis.set_ylim(-0.5, self.min_plotsize - 0.5)
        else:
            axis.set_ylim(-0.5, len(sites) - 0.5)
        max_width = int(axis.get_xlim()[1] * (1 + self.plot_right_margin))
        min_width = axis.get_xlim()[0]
        axis.set_xlim(min_width, max_width)
        axis.set_title('running slots per site')
        axis.set_xlabel('number of slots')
        axis.set_ylabel('site')
        axis.set_yticks(ind)
        axis.set_yticklabels('')
        fontLeg = FontProperties()
        fontLeg.set_size('small')
        axis.legend((bar_1[0], bar_2[0], bar_3[0], bar_4[0]), ('idle slots', 'busy slots', 'suspended slots', 'retiring slots'),
                    loc=6, bbox_to_anchor=(0.8, 0.95), borderaxespad=0., prop = fontLeg)
        plt.grid(axis=y)
        ##########
        # Output #
        ##########
        '''This module contains one plot, one summary subtable,
        one subtable with every information and one subtable to determine the
        different condor versions running on the different sites.'''
        plt.tight_layout()
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + "_sites.png"), dpi=91)
        temp = np.zeros(2)
        temp_2 = np.zeros(2)
        for avg in plot_avg_load_claimed:
            if avg > 0.0:
                temp[0] += avg
                temp[1] += 1
        try:
            claimed_avg = round(temp[0]/temp[1], 2)
        except ZeroDivisionError:
            claimed_avg = 0
        for avg in plot_avg_load_unclaimed:
            if avg > 0:
                temp_2[0] += avg
                temp_2[1] += 1
        try:
            unclaimed_avg = round(temp_2[0]/temp_2[1], 2)
        except ZeroDivisionError:
            claimed_avg = 0
        data["filename_plot"] = self.instance_name + "_sites.png"
        data['claimed_slots'] = state_list.count("Claimed")
        data['unclaimed_slots'] = state_list.count("Unclaimed")
        data['weak_slots'] = sum(plot_weak)
        data['machines'] = len(machine_names)
        data['slots'] = sum(machine_slots)
        data['claimedslots_loadavg'] = claimed_avg
        data['unclaimedslots_loadavg'] = unclaimed_avg
        data['condor_load'] = round(sum(condor_load_list)/len(condor_load_list), 2)

        # Fill Subtables condor_version_per_site
        for i in xrange(len(sites)):
            for j in xrange(len(condor_versions)):
                if sites[i] in condor_version_per_site.keys():  # dont save subtable if site offline
                    if condor_version_per_site[sites[i]][condor_versions[j]] != 0:
                        details_data = {
                            'site': sites[i],
                            'condor_version': condor_versions[j],
                            'value': condor_version_per_site[sites[i]][condor_versions[j]]
                        }
                        self.condor_db_value_list.append(details_data)
        # Fill Subtable statistics
        for i in xrange(slot_count):
            details_data = {
                'mid':      slot_id_list[i],
                'status':   state_list[i],
                'activity': activity_list[i],
                'LoadAvg':  load_list[i]
            }
            self.statistics_db_value_list.append(details_data)

        # Fill Subtable plot
        for i in xrange(len(sites)):
            details_data = {
                'site':           sites[i],
                'claimed':        int(plot_claimed[i]),
                'unclaimed':        int(plot_unclaimed[i]),
                'machines':       int(plot_machines_per_site[i]),
                'claimed_avg':    plot_avg_load_claimed[i],
                'unclaimed_avg':    plot_avg_load_unclaimed[i],
                'disk': plot_disk[i],
                'idle': plot_activity["Idle"][i],
                'busy': plot_activity["Busy"][i],
                'suspended': plot_activity["Suspended"][i],
                'retiring': plot_activity["Retiring"][i]
            }
            self.plot_db_value_list.append(details_data)
        #########################
        # calculation of Status #
        #########################
        if slot_count < self.slots_min:
            data['status'] = 0.5
            data['error_msg'] = "Only " + str(slot_count) + " slots online"
        elif state_list.count("Claimed") == 0:
            data['status'] = 0.5
            data['error_msg'] = "No Slots Claimed"
        else:
            if len(machine_names) * self.machine_slot_min > slot_count:
                data['status'] = 0
                data['error_msg'] = "Only " + \
                    str(round(float(slot_count) / len(machine_names), 1)) + \
                    " slots per machine are active. <br> "
            try:
                temp = float(state_list.count("Claimed")) / float(state_list.count("Unclaimed"))
            except ZeroDivisionError:
                temp = 0
            if temp < self.claimed_unclaimed_ratio and state_list.count("Claimed") > 0:
                data['status'] = 0
                data['error_msg'] = data['error_msg'] + "The ratio between claimed and unclaimed slots is below " + \
                    str(self.claimed_unclaimed_ratio) + ". <br>"

            if claimed_avg < self.weak_threshold and state_list.count("Claimed") > 0:
                data['status'] = 0
                data['error_msg'] = data['error_msg'] + \
                    "The average load of busy and retiring slots is below " + \
                    str(self.weak_threshold) + ". <br>"
            if unclaimed_avg > self.weak_threshold:
                data['status'] = 0
                data['error_msg'] = data['error_msg'] + \
                    "The average load of idle and suspended slots is bigger than " + \
                    str(self.weak_threshold) + ". <br>"
            if float(sum(plot_weak))/state_list.count("Claimed") > self.weak_slots_limit:
                data['status'] = 0
                data['error_msg'] = data['error_msg'] + \
                    "More than " + str(self.weak_slots_limit*100) + " % of busy and retiring slots have a load below " + \
                    str(self.weak_threshold) + ". <br>"
        print data
        return data

    # Putting Data in the Subtable to display
    def fillSubtables(self, parent_id):
        self.subtables['statistics'].insert().execute([dict(parent_id=parent_id, **row)
                                                       for row in self.statistics_db_value_list])
        self.subtables['plot'].insert().execute([dict(parent_id=parent_id, **row)
                                                 for row in self.plot_db_value_list])
        self.subtables['condor'].insert().execute([dict(parent_id=parent_id, **row)
                                                   for row in self.condor_db_value_list])

    # Making Subtable Data available to the html-output
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['statistics'].select().where(
            self.subtables['statistics'].c.parent_id == self.dataset['id']).execute().fetchall()
        data["statistics"] = map(dict, details_list)

        details_list = self.subtables['plot'].select().where(
            self.subtables['plot'].c.parent_id == self.dataset['id']).execute().fetchall()
        data["plot"] = map(dict, details_list)

        details_list = self.subtables['condor'].select().where(
            self.subtables['condor'].c.parent_id == self.dataset['id']).execute().fetchall()
        data["condor"] = map(dict, details_list)
        return data
