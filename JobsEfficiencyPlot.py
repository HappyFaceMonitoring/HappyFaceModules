# -*- coding: utf-8 -*-
#
# Copyright 2012 Institut für Experimentelle Kernphysik - Karlsruher Institut für Technologie
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

import hf, lxml, logging, datetime
import numpy as np 
from numpy import array
from sqlalchemy import *
from lxml import etree

class JobsEfficiencyPlot(hf.module.ModuleBase):
    config_keys = {
        'group': ('Colon separated user groups to include in the output, leave empty for all', ''),
        'qstat_xml': ('URL of the input qstat xml file', ''),
    }
            
    table_columns = [
        Column("filename_eff_plot", TEXT),
        Column("filename_rel_eff_plot", TEXT),
        Column("result_timestamp", INT)
    ], ['filename_eff_plot', 'filename_rel_eff_plot']
    

    def prepareAcquisition(self):
        group = self.config["group"].strip()
        
        self.groups = []
        if group != '': self.groups = group.split(',')
        
        if 'qstat_xml' not in self.config: raise hf.exceptions.ConfigError('qstat_xml option not set')
        self.qstat_xml = hf.downloadService.addDownload(self.config['qstat_xml'])

    def getGroupHierarchy(self, root):
        hierarchy = {}
        for element in root:
            if element.tag == "summaries":
                for child in element:
                    if child.tag == 'summary':
                        group = 'all'
                        if 'group' in child.attrib:
                            group = child.attrib['group']

                        if 'parent' in child.attrib:
                            hierarchy[group] = child.attrib['parent']
                        else:
                            hierarchy[group] = None
        return hierarchy

    def checkGroup(self, group_chk, group, hierarchy):
        try:
            while group_chk != group:
                if hierarchy[group_chk] == None:
                    return False
                group_chk = hierarchy[group_chk]
            return True
        except:
            return False

    def checkGroups(self, group_chk, groups, hierarchy):
        if len(groups) == 0: return True

        for group in groups:
            if self.checkGroup(group_chk, group, hierarchy):
                return True
        return False

    def extractData(self):
        import matplotlib
        import matplotlib.pyplot as plt
        self.plt = plt
        data = {}
        data["filename_eff_plot"] = ""
        data["filename_rel_eff_plot"] = ""
        data["result_timestamp"] = 0
        data['source_url'] = self.qstat_xml.getSourceUrl()
        data['status'] = 1.0
        source_tree = etree.parse(open(self.qstat_xml.getTmpPath()))

        root = source_tree.getroot()
        hierarchy = self.getGroupHierarchy(root)

        # Check input file timestamp
        date = 0
        for element in root:
            if element.tag == "header":
                for child in element:
                    if child.tag == "date":
                        date = int(float(child.text.strip()))

        data["result_timestamp"] = date

        # TODO: Provide a common base class which constructs the users array
        # and the arrays required for JobsDist...
        users = {}
        for element in root:
            if element.tag == "jobs":
                for child in element:

                    user = group = job_state = cpuwallratio = ''
                    for subchild in child:
                        if subchild.tag == 'user':
                            user = subchild.text.strip()
                        if subchild.tag == 'state':
                            job_state = subchild.text.strip()
                        if subchild.tag == 'cpueff':
                            cpuwallratio = subchild.text.strip()
                        if subchild.tag == 'group':
                            group = subchild.text.strip()

                    if user == '':
                        continue

                    # Check group
                    if not self.checkGroups(group, self.groups, hierarchy):
                        continue

                    if user not in users:
                        users[user] = {}

                        users[user]["total"] = 0
                        users[user]["running"] = 0
                        users[user]["waiting"] = 0
                        users[user]["queue"] = 0
                        users[user]["ratio100"] = 0
                        users[user]["ratio80"] = 0
                        users[user]["ratio30"] = 0
                        users[user]["ratio10"] = 0

                    users[user]["total"] += 1

                    if job_state == 'pending': users[user]["queue"] = users[user]["queue"] + 1
                    if job_state == 'waiting': users[user]["waiting"] = users[user]["waiting"] + 1
                    if job_state == 'running':
                        users[user]["running"] += 1
                        # sometimes there is no "cpuwallratio" variable for running jobs
                        if cpuwallratio != '':
                            if float(cpuwallratio) > 80:
                                users[user]["ratio100"] += 1
                            elif float(cpuwallratio) > 30:
                                users[user]["ratio80"] += 1
                            elif float(cpuwallratio) > 10:
                                users[user]["ratio30"] += 1
                            else:
                                users[user]["ratio10"] += 1

        ################################################################
        ### AT THE MOMENT: QUICK AND DIRTY
        ### inspired by: http://matplotlib.sourceforge.net/examples/pylab_examples/bar_stacked.html

        ### muss bei gelegenheit ueberschrieben werden
        ### saubere initialisierung der "figure", "canvas", ...
        ### striktere definitionen der plot-eigenschaften, verschieben der legende
        ### verringerung von code durch auslagerung von funktionen

        N = len(users)

        ### break image creation if there are no user stats about batch jobs
        if N == 0:
            return

        user_names = []
        total = []
        
        ratio100 = []; ratio80 = []; ratio30 = []; ratio10 = []; queue = []
        rel_ratio100 = []; rel_ratio80 = []; rel_ratio30 = []; rel_ratio10 = []; rel_queue = []

        for user in users:
            user_names.append(user)
            total.append(float(users[user]["total"]))
            ratio100.append(float(users[user]["ratio100"]))
            ratio80.append(float(users[user]["ratio80"]))
            ratio30.append(float(users[user]["ratio30"]))
            ratio10.append(float(users[user]["ratio10"]))
            queue.append(float(users[user]["queue"]))

        max_jobs = max(total)
        scale_value = max_jobs // 10
        if scale_value == 0: scale_value = 5


        bot_queue = queue
        bot10 = array(queue) + array(ratio10)
        bot30 = array(queue) + array(ratio10) + array(ratio30)
        bot80 =        array(queue) + array(ratio10) + array(ratio30) + array(ratio80)

        for user in users:
            if users[user]["total"] != 0:
                rel_ratio100.append( round(float(users[user]["ratio100"]) * 100. / float(users[user]["total"]),1) )
                rel_ratio80.append( round(float(users[user]["ratio80"]) * 100. / float(users[user]["total"]),1) )
                rel_ratio30.append( round(float(users[user]["ratio30"]) * 100. / float(users[user]["total"]),1) )
                rel_ratio10.append( round(float(users[user]["ratio10"]) * 100. / float(users[user]["total"]),1) )
                rel_queue.append( round(float(users[user]["queue"]) * 100. / float(users[user]["total"]),1) )
            else:
                rel_ratio100.append(0); rel_ratio80.append(0); rel_ratio30.append(0); rel_ratio10.append(0)

        rel_bot_queue = rel_queue
        rel_bot10 = array(rel_queue) + array(rel_ratio10)
        rel_bot30 = array(rel_queue) + array(rel_ratio10) + array(rel_ratio30)
        rel_bot80 = array(rel_queue) + array(rel_ratio10) + array(rel_ratio30) + array(rel_ratio80)



        ind = np.arange(N)    # the x locations for the groups
        width = 0.36       # the width of the bars: can also be len(x) sequence

        fig_abs = self.plt.figure()
        fig_rel = self.plt.figure()

        #canvas_abs = FigureCanvas(fig_abs)
        #canvas_rel = FigureCanvas(fig_rel)

        axis_abs = fig_abs.add_subplot(111)
        axis_rel = fig_rel.add_subplot(111)
        
        #if N > 1: axis_abs.set_yscale('log')
        
        ##########################################################
        # create first plot, absolute view
        
        p0 = axis_abs.bar(ind, queue,   width, color='violet')
        p1 = axis_abs.bar(ind, ratio10,   width, color='r', bottom = bot_queue)
        p2 = axis_abs.bar(ind, ratio30, width, color='orange', bottom = bot10 )
        p3 = axis_abs.bar(ind, ratio80, width, color='y', bottom = bot30 )
        p4 = axis_abs.bar(ind, ratio100, width, color='g', bottom = bot80 )

        axis_abs.set_position([0.10,0.2,0.85,0.75])
        axis_abs.set_ylabel('Number of Jobs')
        axis_abs.set_title('Job Efficiency (absolute view)')
        axis_abs.set_xticks(ind+width/2.)
        axis_abs.set_xticklabels(user_names, rotation='vertical')
        #if N > 1: axis_abs.set_ylim(1,max_jobs)
        axis_abs.set_yticks(np.arange(0,max_jobs + 5,scale_value))
        axis_abs.legend( (p0[0], p1[0], p2[0], p3[0], p4[0]), ('queue', 'ratio < 10%', '10% < ratio < 30%', '30% < ratio < 80%', 'ratio > 80%') )

        
        fig_abs.savefig(hf.downloadService.getArchivePath(self.run, self.instance_name + "_jobs_eff.png"), dpi=60)
        data["filename_eff_plot"] = self.instance_name + "_jobs_eff.png"

        ##########################################################
        # create second plot, relative view

        rel_p0 = axis_rel.bar(ind, rel_queue,   width, color='violet')
        rel_p1 = axis_rel.bar(ind, rel_ratio10,   width, color='r', bottom = rel_bot_queue)
        rel_p2 = axis_rel.bar(ind, rel_ratio30, width, color='orange', bottom = rel_bot10 )
        rel_p3 = axis_rel.bar(ind, rel_ratio80, width, color='y', bottom = rel_bot30 )
        rel_p4 = axis_rel.bar(ind, rel_ratio100, width, color='g', bottom = rel_bot80 )
        
        axis_rel.set_position([0.10,0.2,0.85,0.75])
        axis_rel.set_ylabel('fraction in %')
        axis_rel.set_title('Job Efficiency (relative view)')
        axis_rel.set_xticks(ind+width/2.)
        axis_rel.set_xticklabels(user_names, rotation='vertical')
        axis_rel.set_yticks(np.arange(0,101,10))

        axis_rel.legend( (rel_p0[0], rel_p1[0], rel_p2[0], rel_p3[0], rel_p4[0]), ('queue', 'ratio < 10%', '10% < ratio < 30%', '30% < ratio < 80%', 'ratio > 80%') )

        fig_rel.savefig(hf.downloadService.getArchivePath(self.run, self.instance_name + "_jobs_rel_eff.png"), dpi=60)
        data["filename_rel_eff_plot"] = self.instance_name + "_jobs_rel_eff.png"
        
        return data
        
