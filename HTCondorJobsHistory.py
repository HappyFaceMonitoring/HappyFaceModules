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
from datetime import timedelta,datetime
class HTCondorJobsHistory(hf.module.ModuleBase):

	config_keys = {
		'source_url' : ('Not used, but filled to avoid errors','http://google.com'),
		'plotsize_x' : ('Size of the plot in x', '8.9'),
		'plotsize_y' : ('Size of the plot in y', '5.8'),
	}

	table_columns = [Column('filename_plot', TEXT)], ['filename_plot']
	
	def prepareAcquisition(self):

		# Setting defaults
		self.source_url = self.config["source_url"]

        	# Define basic structures
	        self.condor_projection = [
			"JobStatus",
			"LastJobStatus",
			"User",
			"RemoteJob",
			"GlobalJobId",
			"CurrentTime",
			"CompletionDate",
			"CommittedSuspensionTime",
			"RequestWalltime",
			"LastRemoteHost",
                        "MachineAttrCloudSite0",
			"QDate",
			"CommittedTime",
			"EnteredCurrentStatus",
			"JobStartDate",
			"ExitCode",
			"ExitBySignal"
	        ]
		self.jobs_status_dict = {1 : "idle", 2 : "running", 3 : "removed", 4 : "completed", 5 : "held", 6 : "transferred", 7 : "suspended"}
		self.jobs_status_colors = {
			"idle" : "#56b4e9",
			"running" : "#009e73",
			"removed" : "firebrick",
			"completed" : "slateblue",
			"held" : "#d55e00",
			"transferred" : "slategrey",
			"suspended" : "#e69f00"
		}

		self.quantities_list = [quantity for quantity in self.condor_projection if quantity != "GlobalJobId"]
		self.condor_jobs_information = {}
		self.jobs_history_statistics = {
			"removed" : [],
			"completed" : []
		}
		self.sites_statistics = {}
		self.walltime_runtime_statistics = {}

		# Prepare htcondor queries
		self.collector = htcondor.Collector()
		self.schedds = [htcondor.Schedd(classAd) for classAd in self.collector.query(htcondor.AdTypes.Schedd)]
		self.schedd_names = [classAd.get("Name") for classAd in self.collector.query(htcondor.AdTypes.Schedd)]
		self.histories = []
		requirement = "RoutedToJobId =?= undefined && JobStartDate > 0 && (EnteredCurrentStatus >= {NOW} - 86400)".format(NOW = int(time.time()))
		for schedd in self.schedds:
			self.histories.append(schedd.history(requirement, self.condor_projection,20000))

	def extractData(self):

		# Initialize the data for the main table
		data = {
			'filename_plot' : ''
		}

		# Extract job information using htcondor python bindings
		for scheddname,history in zip(self.schedd_names,self.histories):
			ad_index = 0
			job_id = "undefined"
			try:
				for ads in history:
					job_id = ads.get("GlobalJobId")
					self.condor_jobs_information[job_id] = {quantity : ads.get(quantity) for quantity in self.quantities_list}
					ad_index += 1
			except RuntimeError:
				print "Failed to get ad for scheduler", scheddname, "after Job ID", job_id,"number",ad_index," --> Aborting"

		# Fill the main table and the user statistics information
		for job in self.condor_jobs_information.itervalues():
			# Determine user and set up user dependent statistics
			user = job["User"]
					# Summarize the status information
			status = self.jobs_status_dict.get(job["JobStatus"])
			if status == "completed":
				# Determine the site where the job was completed
				if job["MachineAttrCloudSite0"]:
					self.sites_statistics.setdefault(job["MachineAttrCloudSite0"].lower(),0)
					self.sites_statistics[job["MachineAttrCloudSite0"].lower()] += 1
				# Determine the runtime and requested walltime of the completed job
				if user not in self.walltime_runtime_statistics:
					self.walltime_runtime_statistics[user] = {}
				if job["CommittedTime"] - job["CommittedSuspensionTime"] >= 0 and job["ExitCode"] == 0 and job["RequestWalltime"]:
					self.walltime_runtime_statistics[user].setdefault(job["RequestWalltime"], []).\
						append(job["CommittedTime"] - job["CommittedSuspensionTime"])
			if job["EnteredCurrentStatus"]:
				self.jobs_history_statistics[status].append(job["EnteredCurrentStatus"])

		# Plot creation for user statistics
		data["filename_plot"] = self.plot()

		return data

	def plot(self):
		import matplotlib.pyplot as plt
		import matplotlib.patches as mpatches
		import matplotlib.markers as markers

		# Create job history plot
		fig_jobhistory = plt.figure(figsize=(float(self.config["plotsize_x"]), float(self.config["plotsize_y"])*1.15))
		axis_jobhistory = fig_jobhistory.add_subplot(111)
		data = [status_list for status_list in self.jobs_history_statistics.itervalues() if len(status_list) > 0]
		labels = [status for status in self.jobs_history_statistics if len(self.jobs_history_statistics[status]) > 0]
		colors = [self.jobs_status_colors[status] for status in labels]
		axis_jobhistory.hist(data, 30, stacked=True, histtype = 'bar', rwidth=1., fill=True, label=labels, color = colors)

		axis_jobhistory.legend()
		axis_jobhistory.set_xlabel('date')
		axis_jobhistory.set_ylabel('number of jobs')
		axis_jobhistory.set_title('Jobs terminated within last 24 hours')

		y_min, y_max = axis_jobhistory.get_ylim()
		axis_jobhistory.set_ylim(y_min,y_max*1.3)
		axis_jobhistory.set_xticklabels([datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S') for t in axis_jobhistory.get_xticks()], rotation=45)

		# Create runtime vs requested walltime plot
		colormap = plt.get_cmap('gist_rainbow')
		colors = [colormap(1.*i/len(self.walltime_runtime_statistics)) for i in range(len(self.walltime_runtime_statistics))]
		fig_walltime_runtime = plt.figure(figsize=(float(self.config["plotsize_x"]), float(self.config["plotsize_y"])))
		axis_walltime_runtime = fig_walltime_runtime.add_subplot(111) 
		axis_walltime_runtime.set_xlim(-3*60*60,27*60*60)
		axis_walltime_runtime.set_ylim(-3*60*60,27*60*60)
		axis_walltime_runtime.grid(True)
		for c,m,user in zip(colors,markers.MarkerStyle.filled_markers,self.walltime_runtime_statistics):
			user_outliers_walltimes = []
			user_outliers_runtimes = []
			for walltime in self.walltime_runtime_statistics[user]:
				per_down = np.percentile(self.walltime_runtime_statistics[user][walltime], 2.5)
				per_50 = np.median(self.walltime_runtime_statistics[user][walltime])
				per_up = np.percentile(self.walltime_runtime_statistics[user][walltime], 97.5)
				axis_walltime_runtime.errorbar(
					[walltime],
					[per_50],
					yerr=[[per_50-per_down],
					[per_up-per_50]],
					color = c,
					ecolor = 'black',
					marker = m,
					markersize = 10,
					markeredgecolor = 'black',
					markeredgewidth = 1.5,
					linewidth = 1.5,
					capthick = 1.5, 
					capsize = 10
				)
				outlier_runtimes = [runtime for runtime in self.walltime_runtime_statistics[user][walltime] if (runtime < per_down or runtime > per_up)]
				outlier_walltimes = [walltime for i in range(len(outlier_runtimes))]
				user_outliers_walltimes += outlier_walltimes
				user_outliers_runtimes += outlier_runtimes
			if len(self.walltime_runtime_statistics[user]) > 0:
				axis_walltime_runtime.scatter(
					user_outliers_walltimes,
					user_outliers_runtimes,
					label=user,
					color = c,
					s = 150,
					marker = m)
		axis_walltime_runtime.plot([-2.75*60*60,27*60*60], [-3*60*60,27*60*60], marker="", color="green")
		time_ticks = [3*60*60*i for i in range(-1,10)]
		axis_walltime_runtime.set_xticks(time_ticks)
		axis_walltime_runtime.set_yticks(time_ticks)
		axis_walltime_runtime.set_xticklabels([timedelta(seconds = t) if (t >= 0 and t <= 86400) else "" for t in time_ticks], rotation = 45)
		axis_walltime_runtime.set_yticklabels([timedelta(seconds = t) if (t >= 0 and t <= 86400) else "" for t in time_ticks])

		axis_walltime_runtime.legend(loc = 'upper center')
		axis_walltime_runtime.set_xlabel('requested walltime')
		axis_walltime_runtime.set_ylabel('runtime')
		axis_walltime_runtime.set_title('Runtime vs. requested Walltime for jobs successfully completed within last 24 hours')
		axis_walltime_runtime.text(-2*60*60,60*60*20, '50 +/- 47.5% percentiles\nfor jobs grouped by user &\nrequested walltime with\nexplicitly shown outliers')

		# Create plot of completed jobs per site
		max_njobs = max([n_jobs for n_jobs in self.sites_statistics.itervalues()])
		fig_completedjobs_site = plt.figure(figsize=(float(self.config["plotsize_x"])*0.9, float(self.config["plotsize_y"])))
		axis_completedjobs_site = fig_completedjobs_site.add_subplot(111) 
		axis_completedjobs_site.set_xlim(-0.5, len(self.sites_statistics)-0.5)
		for index,site in enumerate(self.sites_statistics):
			axis_completedjobs_site.bar(index, float(self.sites_statistics[site]), color = self.jobs_status_colors["completed"], align = 'center', width=0.5)
			axis_completedjobs_site.text(index, float(self.sites_statistics[site])+max_njobs*0.04, str(self.sites_statistics[site]) + " Jobs", ha ='center', va = "center")

		axis_completedjobs_site.set_xticks(range(len(self.sites_statistics)))
		axis_completedjobs_site.set_xticklabels([site for site in self.sites_statistics])
		axis_completedjobs_site.set_ylabel("number of completed jobs")
		axis_completedjobs_site.set_title("Jobs completed within last 24 hours for available sites")
		y_min, y_max = axis_completedjobs_site.get_ylim()
		axis_completedjobs_site.set_ylim(y_min, y_max*1.2)

		# save figures
		plotname = hf.downloadService.getArchivePath(self.run, self.instance_name) 
		plt.tight_layout()
		fig_jobhistory.savefig(plotname + "_jobs_terminated.png", dpi=91, bbox_inches="tight")
		fig_walltime_runtime.savefig(plotname + "_walltime_runtime.png", dpi=91, bbox_inches="tight")
		fig_completedjobs_site.savefig(plotname + "_jobs_site.png", dpi=91, bbox_inches="tight")
		
		return plotname
