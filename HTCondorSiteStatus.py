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
from sqlalchemy import TEXT, INT, FLOAT, Column
import numpy as np
import htcondor
import copy

class HTCondorSiteStatus(hf.module.ModuleBase):


	config_keys = {
		'source_url' : ('Not used, but filled to avoid errors','http://google.com'),
		'plotsize_x' : ('Size of the plot in x', '10.9'),
		'plotsize_y' : ('Size of the plot in y', '5.8'),
		'log_limit' : ('Upper threshold for the amount of jobs with certain status, above which log scale is used', '500'),
	}

        table_columns = [
                Column('claimed', INT),
                Column('unclaimed', INT),
		Column('total',INT),
                Column('underused', INT),
                Column('machines', INT),
                Column('average_load', FLOAT),
                Column('filename_plot', TEXT)
        ], ['filename_plot']

        subtable_columns = {
                'statistics' : ([
                        Column("cloudsite", TEXT),
                        Column("idle", INT),
                        Column("busy", INT),
                        Column("suspended", INT),
                        Column("vacating", INT),
                        Column("killing", INT),
                        Column("benchmarking", INT),
                        Column("retiring", INT),
                        Column("machines", INT)], [])
        }

        def prepareAcquisition(self):

                # Setting defaults
                self.source_url = self.config["source_url"]

                # Define basic structures
                self.condor_projection = [
                        "CloudSite",
                        "Name",
                        "State",
                        "Activity",
                        "Machine",
                        "LoadAvg"
                ]
		self.quantities_list = [quantity for quantity in self.condor_projection if quantity != "Name"]
		self.condor_cloudsites_information = {}

		self.cloudsite_statistics_dict = {
			'machines' : set(),
			'idle' : 0,
			'busy' : 0,
			'suspended' : 0,
			'vacating' : 0,
			'killing' : 0,
			'benchmarking' : 0,
			'retiring' : 0
		}
		self.cloudsite_statistics = {}
		self.cloudsite_activity_colordict = {
			'idle' : '#56b4e9',
			'busy' : '#009e73',
			'suspended' : '#e69f00',
			'vacating' : '#d55e00',
			'killing' : 'firebrick',
			'benchmarking' : 'slategrey',
			'retiring' : 'slateblue'
		}

		# Prepare htcondor queries
		self.collector = htcondor.Collector()

		# Prepare subtable list for database
		self.statistics_db_value_list = []
		return

        def extractData(self):
		data = {
			'claimed' : 0,
			'unclaimed' : 0,
			'total' : 0,
			'underused' : 0,
			'machines' : 0,
			'average_load' : [], # will be changed to a float: mean value over the list of floats created in the following
			'filename_plot' : ''
		}

		# Extract site information using htcondor python bindings
		result = self.collector.query(ad_type = htcondor.AdTypes.Startd, constraint = "RoutedToJobId =?= undefined && Cpus > 0", projection = self.condor_projection)

		# Fill the main table and the cloud site statistics information
		for slot in result:
			# Determine cloud site and set up cloud site dependent statistics
			cloudsite = slot["CloudSite"].lower()
			if cloudsite not in self.cloudsite_statistics:
				self.cloudsite_statistics[cloudsite] = copy.deepcopy(self.cloudsite_statistics_dict)
			# Summarize cloud site activity information
			activity = slot["Activity"].lower()
			self.cloudsite_statistics[cloudsite][activity] += 1
			# Summarize the different slot states of interest
			data['total'] += 1
			slotstate = slot["State"].lower()
			if slotstate in data:
				data[slotstate] += 1
			# Determine unique machine names of the slots and add them to the corresponding set of the cloudsite
			self.cloudsite_statistics[cloudsite]["machines"].add(slot["Machine"])
			# Determine the average load of the slot
			load = slot["LoadAvg"]
			data['average_load'].append(load) 
			if load < 0.5 and activity == 'busy':
				data['underused'] += 1

		for cloudsite,cloudsite_stats in self.cloudsite_statistics.iteritems():
			cloudsite_data = {
				'cloudsite' : cloudsite,
				'machines' : len(cloudsite_stats["machines"]),
				'idle' : cloudsite_stats["idle"],
				'busy' : cloudsite_stats["busy"],
				'suspended' : cloudsite_stats["suspended"],
				'vacating' : cloudsite_stats["vacating"],
				'killing' : cloudsite_stats["killing"],
				'benchmarking' : cloudsite_stats["benchmarking"],
				'retiring' : cloudsite_stats["retiring"]
			}
			self.statistics_db_value_list.append(cloudsite_data)
			data['machines'] += len(cloudsite_stats["machines"])

		data['average_load'] = round(np.mean(data['average_load']),3)*100 if len(data['average_load']) > 0 else 0.0

		# Plot creation for cloud site statistics
		data['filename_plot'] = self.plot()

		return data

        def fillSubtables(self, parent_id):
                self.subtables['statistics'].insert().execute([dict(parent_id=parent_id, **row) for row in self.statistics_db_value_list])


        def getTemplateData(self):

                data = hf.module.ModuleBase.getTemplateData(self)
                statistics_list = self.subtables['statistics'].select().\
                        where(self.subtables['statistics'].c.parent_id == self.dataset['id']).execute().fetchall()
                data['statistics'] = map(dict, statistics_list)
                return data

	def plot(self):
		import matplotlib.pyplot as plt
		import matplotlib.patches as mpatches
		# initializing figure
		fig = plt.figure(figsize=(float(self.config["plotsize_x"]), float(self.config["plotsize_y"])))
		axis = fig.add_subplot(111)
		# determining, whether log x-axis i needed
		max_number = max([cloudsite_stats[activity] for cloudsite_stats in self.cloudsite_statistics.itervalues() for activity in self.cloudsite_activity_colordict])
		log_needed = max_number > float(self.config["log_limit"])
		# creating bar entries for each status and user
		offset = 0.9 if log_needed else 0
		for index,cloudsite in enumerate(self.cloudsite_statistics):
			cloudsite_info = cloudsite + " - " + str(sum([self.cloudsite_statistics[cloudsite][act] for act in self.cloudsite_activity_colordict])) + " Slots"
			axis.text(offset+0.1, index+0.45, cloudsite_info, ha="left", va="center")
			previous_status_value = offset
			for activity,color in self.cloudsite_activity_colordict.iteritems():
				if self.cloudsite_statistics[cloudsite][activity] != 0:
					axis.barh(index,float(self.cloudsite_statistics[cloudsite][activity]),0.5,previous_status_value,color=color,align='center',log=log_needed)
					if previous_status_value == offset:
						previous_status_value = 0
					previous_status_value += float(self.cloudsite_statistics[cloudsite][activity])
		# Creating figure legend
		activity_label_objects = []
		x_max = axis.get_xlim()[1]
		axis.set_ylim(-1,len(self.cloudsite_statistics))
		for activity,color in self.cloudsite_activity_colordict.iteritems():
			activity_label_objects.append(mpatches.Patch(facecolor=color, label=activity, edgecolor="black"))
		axis.legend(activity_label_objects, [o.get_label() for o in activity_label_objects], loc="upper right")
		# Optimizing figure
		x_max *=30 if log_needed else 1.3
		axis.set_xlim(offset, x_max)
		axis.set_title("slots per site")
		axis.set_xlabel("number of slots")
		axis.set_ylabel("site")
		axis.set_yticks([])
		# save figure
		plotname = hf.downloadService.getArchivePath( self.run, self.instance_name + "_siteinfo.png")
		fig.savefig(plotname, dpi=91, bbox_inches="tight")
		return plotname
