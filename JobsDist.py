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

class JobsDist(hf.module.ModuleBase):
    config_keys = {
        'groups': ('Colon separated user groups to include in the output, leave empty for all', ''),
        'variable': ('Select the variable to plot', ''),
        'qstat_xml': ('URL of the input qstat xml file', ''),
    }
            
    table_columns = [
        Column("filename_eff_plot", TEXT),
        Column("result_timestamp", INT),
    ], ['filename_eff_plot']
    

    def prepareAcquisition(self):
        # read configuration
        self.variable = self.config['variable']
        group = self.config["groups"].strip()
        if group != '': self.groups = group.split(',')
        else: self.groups = group
        self.splitnum = 1
        
        if self.variable == 'cputime' or self.variable == 'walltime':
            self.splitnum = 3

        if len(self.groups) == 0 or self.groups[0] == '':
            self.groups = []
        
        if 'qstat_xml' not in self.config: raise hf.exceptions.ConfigError('qstat_xml option not set')
        self.qstat_xml = hf.downloadService.addDownload(self.config['qstat_xml'])
        

    def extractData(self):
        import matplotlib
        import matplotlib.pyplot as plt
        self.plt = plt
        data = {}
        data["filename_eff_plot"] = ""
        data["result_timestamp"] = 0
        data['source_url'] = self.qstat_xml.getSourceUrl()
        data['status'] = 1.0

        source_tree = etree.parse(open(self.qstat_xml.getTmpPath()))
        root = source_tree.getroot()

        values = []
        variable = self.variable
        nbins = 20

        hierarchy = self.getGroupHierarchy(root)

        # Check input file timestamp
        date = 0
        for element in root:
            if element.tag == "header":
                for child in element:
                    if child.tag == "date" and child.text is not None:
                        date = int(float(child.text.strip()))

        data["result_timestamp"] = date
        data['status'] = 1.0
#        if self.timestamp - date > self.old_result_critical_limit*3600:
#            self.status = 0.0
#        elif self.timestamp - date > self.old_result_warning_limit*3600:
#            self.status = 0.5

        for element in root:
            if element.tag == "jobs":
                for child in element:
                    group = job_state = variable_str = ''
                    # Only count running jobs
                    for subchild in child:
                        if subchild.tag == 'group' and subchild.text is not None:
                            group = subchild.text.strip()
                        if subchild.tag == 'state' and subchild.text is not None:
                            job_state = subchild.text.strip()
                        if subchild.tag == variable and subchild.text is not None:
                            variable_str = subchild.text.strip()

                    # Check user
                    if not self.checkGroups(group, self.groups, hierarchy) or job_state != 'running' or variable_str == '':
                            continue

                    values.append(float(variable_str))
################################################################
        ### AT THE MOMENT: QUICK AND DIRTY
        ### inspired by: http://matplotlib.sourceforge.net/examples/pylab_examples/bar_stacked.html

        ### muss bei gelegenheit ueberschrieben werden
        ### saubere initialisierung der "figure", "canvas", ...
        ### striktere definitionen der plot-eigenschaften, verschieben der legende
        ### verringerung von code durch auslagerung von funktionen

        ### break image creation if there are no jobs
        if len(values) == 0:
            data['error_string'] = "There are no '%s' jobs running" % self.config["groups"]
            data["filename_eff_plot"] = ""
        else:
            min_var = min(values)
            max_var = max(values)
            diff_var = max_var - min_var

            # Show only one bin in case there is only one value or all values are
            # equivalent
            if diff_var == 0:
                nbins = 1

            content = [0]*nbins
            for value in values:
                if diff_var > 0:
                    bin = int(round((value - min_var) * nbins / diff_var))
                else:
                    bin = nbins;

                if bin == nbins:
                    bin = nbins - 1
                content[bin] += 1

            xlabels = [0]*nbins
            for x in range(0,nbins):
                num = min_var + (x + 0.5)/nbins * diff_var
                int_num = int(num + 0.5)

                xlabelvalues = []
                for y in range(0, self.splitnum):
                    c = int_num / (60**y)
                    cstr = ''
                    if y < self.splitnum-1:
                        cstr = "%02d" % (c % 60)
                    else:
                        cstr = str(c)
                    xlabelvalues.append(cstr)

                xlabelvalues.reverse()
                xlabels[x] = ':'.join(xlabelvalues)

            max_bin_height = max(content);
            scale_value = max_bin_height // 10
            if scale_value == 0: scale_value = 5

            ind = np.arange(nbins)    # the x locations for the groups
            width = 1.00       # the width of the bars: can also be len(x) sequence

            fig = self.plt.figure()

            axis = fig.add_subplot(111)

            p0 = axis.bar(ind, content, width, color='orange')

            axis.set_position([0.10,0.2,0.85,0.75])
            axis.set_xlabel(variable);
            axis.set_ylabel('Number of Jobs')
            axis.set_title(variable + ' distribution')
            axis.set_xticks(ind + width / 2.0)
            axis.set_xticklabels(xlabels, rotation='vertical')
            axis.set_yticks(np.arange(0,max_bin_height + 5,scale_value))

            fig.savefig(hf.downloadService.getArchivePath(self.run, self.instance_name + "_jobs_dist.png"), dpi=60)
            data["filename_eff_plot"] = self.instance_name + "_jobs_dist.png"
        return data
        
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