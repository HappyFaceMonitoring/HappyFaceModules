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
import htcondor
import re
import copy
import time
import numpy as np
from datetime import timedelta
class HTCondorJobsPerUser(hf.module.ModuleBase):

	config_keys = {
		'source_url' : ('Not used, but filled to avoid errors','http://google.com'),
		'plotsize_x' : ('Size of the plot in x', '10.9'),
		'plotsize_y' : ('Size of the plot in y', '5.8'),
		'log_limit' : ('Upper threshold for the amount of jobs with certain status, above which log scale is used', '500'),
		'efficiency_warning' : ('Lower threshold for the averaged efficiencies, below which a warning is given', '0.9'),
		'efficiency_critical': ('Lower threshold for the averaged efficiencies, below which the status is critical', '0.5'),
		'n_held_warning' : ('Upper threshold for the number of held jobs, above which which a warning is given', '10'),
		'n_held_critical' : ('Upper threshold for the number of held jobs, above which which the status is critical', '20'),
		'ram_n_cores_ratio' : ('Upper threshold for the ratio between the ram and n_cores, above which a warning is given','5')
	}

	table_columns = [
		Column('running', INT),
		Column('idle', INT),
		Column('cores', INT),
		Column('ram', INT),
		Column('efficiency', FLOAT),
		Column('qtime', TEXT),
		Column('remote', INT),
		Column('filename_plot', TEXT)
	], ['filename_plot']

	subtable_columns = {
		'statistics' : ([
			Column("user", TEXT),
			Column("idle", INT),
			Column("running", INT),
			Column("removed", INT),
			Column("completed", INT),
			Column("held", INT),
			Column("transferred", INT),
			Column("suspended", INT),
			Column("cores", INT),
			Column("efficiency", FLOAT),
			Column("walltime", INT),
			Column("runtime", INT),
			Column("sites", TEXT),
			Column("ram", INT)], [])
	}

	diskspace_units_dict = {"B" : 0, "KiB" : 1, "MiB" : 2, "GiB" : 3, "TiB" : 4}

	def prepareAcquisition(self):

		# Setting defaults
		self.source_url = self.config["source_url"]

        	# Define basic structures
	        self.condor_projection = [
			"JobStatus",
			"User",
			"ImageSize_raw",
			"RequestCpus",
			"RemoteJob",
			"GlobalJobId",
			"RemoteSysCpu",
			"RemoteUserCpu",
			"CurrentTime",
			"RemoteHost",
			"QDate",
			"JobCurrentStartDate",
			"JobStartDate"
	        ]
		self.jobs_status_dict = {1 : "idle", 2 : "running", 3 : "removed", 4 : "completed", 5 : "held", 6 : "transferred", 7 : "suspended"}
		self.jobs_status_colors = ["#56b4e9", "#009e73", "firebrick", "slateblue", "#d55e00", "slategrey", "#e69f00"]
		self.sites_dict = {
			".*ekp(?:blus|condor|lx\d+).+" : "condocker",
			".*bwforcluster.*" : "bwforcluster",
			".*ekps(?:g|m)\d+.*" : "ekpsupermachines"
		}

		self.quantities_list = [quantity for quantity in self.condor_projection if quantity != "GlobalJobId"]
		self.condor_jobs_information = {}
		self.user_statistics_dict = {
			"removed" : 0,
			"completed" : 0,
			"transferred" : 0,
			"suspended" : 0,
			"held" : 0,
			"idle": 0,
			"running" : 0,
			"cores" : 0,
                        "efficiencies" : [],
			"runtimes" : [],
			"cputimes" : [],
			"walltimes" : [],
                        "sites" : [],
			"ram" : 0
		}
		self.user_statistics = {}

		# Prepare htcondor queries
		self.collector = htcondor.Collector()
		self.schedds = [htcondor.Schedd(classAd) for classAd in self.collector.query(htcondor.AdTypes.Schedd)]
		self.queries = []
		for schedd in self.schedds:
			self.queries.append(schedd.xquery(requirements = "RoutedToJobId =?= undefined", projection = self.condor_projection))

		# Prepare subtable list for database
		self.statistics_db_value_list = []

	def extractData(self):

		# Initialize the data for the main table
		data = {
			'running': 0,
                	'idle': 0,
                	'cores': 0,
                	'ram': 0,
                	'efficiency': 0,
                	'qtime': [], # format changed to 'int' in the end
                	'remote': 0,
			'filename_plot' : ''
		}

		# Extract job information using htcondor python bindings
		for query in htcondor.poll(self.queries):
			for ads in query:
				job_id = ads.get("GlobalJobId")
				self.condor_jobs_information[job_id] = {quantity : ads.get(quantity) for quantity in self.quantities_list}

		# Fill the main table and the user statistics information
		for job in self.condor_jobs_information.itervalues():
			# Count remotable jobs
			if job["RemoteJob"]:
				data["remote"] += 1
			# Determine user and set up user dependent statistics
			user = job["User"]
			if user not in self.user_statistics:
				self.user_statistics[user] = copy.deepcopy(self.user_statistics_dict)
			# Count used RAM in KiB
			self.user_statistics[user]["ram"] += job["ImageSize_raw"]
			data["ram"] += job["ImageSize_raw"]
			# Count used cores
			self.user_statistics[user]["cores"] += job["RequestCpus"]
			data["cores"] += job["RequestCpus"]
			# Summarize the status information
			status = self.jobs_status_dict.get(job["JobStatus"])
			self.user_statistics[user][status] +=1
			if status in data:
				data[status] += 1
			# Calculate the time in the queue for all jobs in seconds
			if status == "running": 
				try:
					data["qtime"].append(max(0, job["JobStartDate"] - job["QDate"]))
				except Exception:
					pass
			# Determine the sites the user is running his jobs on
			job["RemoteHost"] = "Undefined" if job["RemoteHost"] is None else job["RemoteHost"]
			for site_regex in self.sites_dict:
				if re.match(re.compile(site_regex), job["RemoteHost"]) and self.sites_dict[site_regex] not in self.user_statistics[user]["sites"]:
					self.user_statistics[user]["sites"].append(self.sites_dict[site_regex])
			# Calculate runtimes, cputimes and efficiencies of each job of a user
			if status == "running":
				cputime = job["RemoteUserCpu"] + job["RemoteSysCpu"]
				runtime = int(time.time())- job["JobCurrentStartDate"]
				efficiency = float(cputime)/float(runtime)
				# Avoiding not up to date values of JobCurrentStartDate, that result in efficiencies bigger than 1
				if efficiency <= 1.:
					self.user_statistics[user]["efficiencies"].append(efficiency)

		all_efficiencies = []
		for user in self.user_statistics:
			user_data = {"user": user}
			for status in self.jobs_status_dict.itervalues():
				user_data[status] = self.user_statistics[user][status]
			user_data["cores"],user_data["ram"] = self.user_statistics[user]["cores"], self.determine_diskspace(self.user_statistics[user]["ram"])
			user_data["efficiency"] = round(np.mean(self.user_statistics[user]["efficiencies"]),2) \
				if len(self.user_statistics[user]["efficiencies"]) > 0 else 1.0
			all_efficiencies += self.user_statistics[user]["efficiencies"]
			user_data["sites"] = ",\n".join(self.user_statistics[user]["sites"])
			self.statistics_db_value_list.append(user_data)

		data["efficiency"] = round(np.mean(all_efficiencies),2) if len(all_efficiencies)> 0 else 1.0
		data["ram"] = self.determine_diskspace(data["ram"])
		data["qtime"] = str(timedelta(seconds=int(np.mean(data["qtime"]))))

		# Plot creation for user statistics
		data["filename_plot"] = self.plot()

		# Overall status calculation
		efficiency_status = 0.0
		if data["efficiency"] >= float(self.config["efficiency_warning"]):
			efficiency_status = 1.0
		elif data["efficiency"] >= float(self.config["efficiency_critical"]):
			efficiency_status = 0.5
		ram_status = 0.5 if float(data["ram"])/float(data["cores"]) > float(self.config["ram_n_cores_ratio"]) else 1.0
		queue_time_status = 0.0 if "day" in data["qtime"] else 1.0
		print efficiency_status, ram_status, queue_time_status
		data["status"] = min(efficiency_status, ram_status, queue_time_status)
		return data


	def fillSubtables(self, parent_id):
		self.subtables['statistics'].insert().execute([dict(parent_id=parent_id, **row) for row in self.statistics_db_value_list])

	def getTemplateData(self):  

		data = hf.module.ModuleBase.getTemplateData(self)
		statistics_list = self.subtables['statistics'].select().\
			where(self.subtables['statistics'].c.parent_id == self.dataset['id']).execute().fetchall()
		data["statistics"] = map(dict, statistics_list)
		return data

	# module specific functions
	@classmethod
	def determine_diskspace(cls, given_number, given_unit ="KiB", desired_unit ="GiB"):
		orders_to_transform = cls.diskspace_units_dict[desired_unit] - cls.diskspace_units_dict[given_unit]
		return int(round(given_number/1024.0**orders_to_transform, 1))

	def plot(self):
		import matplotlib.pyplot as plt
		import matplotlib.patches as mpatches
		# initializing figure
		fig = plt.figure(figsize=(float(self.config["plotsize_x"]), float(self.config["plotsize_y"])))
		axis = fig.add_subplot(111)
		# determining, whether log x-axis i needed
		max_number = max([user_stats[status] for user_stats in self.user_statistics.itervalues() for status in self.jobs_status_dict.itervalues()])
		log_needed = max_number > float(self.config["log_limit"])
		# creating bar entries for each status and user
		offset = 0.9 if log_needed else 0
		for index,user in enumerate(self.user_statistics):
			user_info = user + " - " + str(sum([self.user_statistics[user][st] for st in self.jobs_status_dict.itervalues()])) + " Jobs"
			axis.text(offset+0.1, index+0.45, user_info, ha="left", va="center")
			previous_status_value = offset
			for color,jobstatus in zip(self.jobs_status_colors,self.jobs_status_dict.itervalues()):
				if self.user_statistics[user][jobstatus] != 0:
					axis.barh(index,float(self.user_statistics[user][jobstatus]),0.5,previous_status_value,color=color,align='center',log=log_needed)
					if previous_status_value == offset:
						previous_status_value = 0
					previous_status_value += float(self.user_statistics[user][jobstatus])
		# Creating figure legend
		status_label_objects = []
		x_max = axis.get_xlim()[1]
		axis.set_ylim(-1,len(self.user_statistics))
		for color,jobstatus in zip(self.jobs_status_colors,self.jobs_status_dict.itervalues()):
			status_label_objects.append(mpatches.Patch(facecolor=color, label=jobstatus, edgecolor="black"))
		axis.legend(status_label_objects, [o.get_label() for o in status_label_objects], loc="upper right")
		# Optimizing figure
		x_max *=30 if log_needed else 1.3
		axis.set_xlim(offset, x_max)
		axis.set_title("jobs per user")
		axis.set_xlabel("number of jobs")
		axis.set_ylabel("user")
		axis.set_yticks([])
		# save figure
		plotname = hf.downloadService.getArchivePath( self.run, self.instance_name + "_userinfo.png")
		fig.savefig(plotname, dpi=91, bbox_inches="tight")
		return plotname
