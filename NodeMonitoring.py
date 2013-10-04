# -*- coding: utf-8 -*-
#
# Copyright 2013 Institut für Experimentelle Kernphysik - Karlsruher Institut für Technologie
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
import lxml.html
from lxml.html.clean import clean_html
import StringIO
from sqlalchemy import *
import numpy as np 
from numpy import array
from datetime import datetime
import pytz
import json

class NodeMonitoring(hf.module.ModuleBase):

    config_keys = {
        'source_url': ('URL to XML data source', ''),
        'primary_key': ('used to uniquely identify different worker nodes', ''),
        'secondary_key': ('additional information for worker node, displayed in \
                parentheses', ''),
        'attribute': ('attribute that is to be analyzed and displayed', ''),
        'attribute_values': ('pipe-separated list of attribute values that are \
                to be considered, if left blank all attribute values that appear \
                in the source are used', ''),
        'eval_attribute_value': ('attribute value that is to be used for \
                evaluation, if left blank all attribute values are summed \
                over', ''),
        'eval_threshold': ('number of jobs above which a status is calculated, \
                set to -1 to disable evaluation', ''),
        'eval_threshold_warning': ('number of jobs above which status is set to \
                warning, set to -1 to disable warning', ''),
        'eval_threshold_critical': ('number of jobs above which status is set to \
                critical, set to -1 to disable critical', ''),
        'plot_filter_node_number': ('maximum number of worker nodes that are to \
                appear in the plot', ''),
        'plot_filter_attribute_value': ('attribute value according to which the \
                worker nodes are sorted and displayed in the plot, if left blank, \
                the nodes are sorted according to the total number of jobs for \
                all existing attribute values', ''),
        'plot_line_warning': ('set to 1 to draw a line indicating the warning \
                threshold in the plot, set to 0 to hide this line', ''),
        'plot_line_critical': ('set to 1 to draw a line indicating the critical \
                threshold in the plot, set to 0 to hide this line', '')
    }
    config_hint = ''
    
    table_columns = ([
        Column("InstanceTitle", TEXT),
        Column("filename_plot", TEXT),
        Column('IntervalStart', TEXT),
        Column('IntervalEnd', TEXT),
        Column('CurrentHourStart', TEXT),
        Column('CurrentHourEnd', TEXT),
        Column('LastHourStart', TEXT),
        Column('LastHourEnd', TEXT)], ['filename_plot'])

    subtable_columns = {
        'statistics':([
            Column('CatName', TEXT),
            Column('JobsCurrentHour', INT),
            Column('JobFracsCurrentHour', FLOAT),
            Column('JobsLastHour', INT),
            Column('JobFracsLastHour', FLOAT),
            Column('MinJobs', INT),
            Column('MaxJobs', INT),
            Column('AvgJobs', INT)], [])}

    def prepareAcquisition(self):
        try:
            self.primary_key = self.config['primary_key']
            self.secondary_key = self.config['secondary_key']
            self.attribute = self.config['attribute']
            self.attribute_values = self.config['attribute_values']
            self.eval_attribute_value = self.config['eval_attribute_value']
            self.eval_threshold = int(self.config['eval_threshold'])
            self.eval_threshold_warning = int(self.config['eval_threshold_warning_percentage'])
            self.eval_threshold_critical = int(self.config['eval_threshold_critical_percentage'])
            self.plot_filter_node_number = int(self.config['plot_filter_node_number'])
            self.plot_filter_attribute_value = self.config['plot_filter_attribute_value']
            self.plot_line_warning = int(self.config['plot_line_warning'])
            self.plot_line_critical = int(self.config['plot_line_critical'])
        except KeyError, ex:
            raise hf.exceptions.ConfigError('Required parameter "%s" not specified' % str(e))
        self.use_secondary_key = self.secondary_key <> ''
        if 'source_url' not in self.config:
            raise hf.exceptions.ConfigError('No source URL specified')
        self.source = hf.downloadService.addDownload(self.config['source_url'])
        self.source_url = []
        self.source_url.append(self.source.getSourceUrl())
        self.statistics_db_value_list = []
  
    def extractData(self):

        import matplotlib
        import matplotlib.pyplot as plt
        import matplotlib.cm as cm
        from matplotlib.font_manager import FontProperties
        self.plt = plt

        data = {}
        data["filename_plot"] = ""
        
        with open(self.source.getTmpPath()) as webpage:
            rawdata = json.load(webpage)

        # Prepare different attribute values (either use those indicated in
        # config file or loop over data and get all different categories)
        AttributeValues = []
        if self.eval_attribute_value <> '':
            AttributeValues.append(self.eval_attribute_value)
        if self.attribute_values <> '':
            AddAttributeValues = self.attribute_values.split('|')
            for i in range(len(AddAttributeValues)):
                if AddAttributeValues[i] not in AttributeValues:
                    AttributeValues.append(AddAttributeValues[i])
        else:
            for item in rawdata['jobs']:
                if item[self.attribute] not in AttributeValues:
                    AttributeValues.append(item[self.attribute])

        # Get all different primary and secondary keys for the selected attribute values
        PrimaryKeys = []
        SecondaryKeys = []
        for item in rawdata['jobs']:
            if item[self.attribute] in AttributeValues:
                if item[self.primary_key] not in PrimaryKeys:
                    PrimaryKeys.append(item[self.primary_key])
                    if self.use_secondary_key == True:
                        SecondaryKeys.append(item[self.secondary_key])

        # Get job numbers from raw data
        Jobs = [[0 for k in range(len(PrimaryKeys))] for a in range(len(AttributeValues))]
        for item in rawdata['jobs']:
            if item[self.attribute] in AttributeValues:
                Jobs[AttributeValues.index(item[self.attribute])][
                        PrimaryKeys.index(item[self.primary_key])] += 1

        # Get total number of jobs across all categories per node
        TotalJobsPerNode = [0 for k in range(len(PrimaryKeys))]
        for k in range(len(PrimaryKeys)):
            for a in range(len(AttributeValues)):
                TotalJobsPerNode[k] += Jobs[a][k]
                
        # Function to convert raw time data given in UTC to local time zone
        def ChangeTimeZone(TimeStringIn, InFormatString, OutFormatString):
            Date = datetime.strptime(TimeStringIn, InFormatString).replace(
                    tzinfo=pytz.utc).astimezone(pytz.timezone('Europe/Berlin'))
            return(Date.strftime(OutFormatString))

        data['IntervalStart'] = ChangeTimeZone(rawdata['meta']['date1'][0], 
                "%Y-%m-%d %H:%M:%S", "%d-%b-%y %H:%M:%S")
        data['IntervalEnd'] = ChangeTimeZone(rawdata['meta']['date2'][0], 
                "%Y-%m-%d %H:%M:%S", "%d-%b-%y %H:%M:%S")
        
        # Calculate module status
        data['status'] = 1.0
        if self.eval_threshold > -1:
            if self.eval_attribute_value <> '':
                i = AttributeValues.index(self.eval_attribute_value)
                Statistics = [Jobs[i][k] for k in range(len(PrimaryKeys))]
            else:
                Statistics = [TotalJobsPerNode[k] for k in range(len(PrimaryKeys))]
            TotalEval = 0
            for k in range(len(PrimaryKeys)):
                TotalEval += Statistics[k]
            if TotalEval >= self.eval_threshold:
                if self.eval_threshold_warning > -1:
                    for k in range(len(PrimaryKeys)):
                        if 100.0 * Statistics[k] / float(TotalEval) >= float(
                                self.eval_threshold_warning):
                            data['status'] = 0.5
                if self.eval_threshold_critical > -1:
                    for k in range(len(PrimaryKeys)):
                        if 100.0 * Statistics[k] / float(TotalEval) >= float(
                                self.eval_threshold_critical):
                            data['status'] = 0.0

        ################################################################
        ### Plot data

        # Get filtered subset of job numbers to plot
        PlotIndices = []
        if self.plot_filter_attribute_value in AttributeValues:
            AttributeValueIndex = AttributeValues.index(
                    self.plot_filter_attribute_value)
            CountsSet = set(Jobs[AttributeValueIndex])
            Counts = [c for c in CountsSet]
            Counts.sort(reverse=True)
            for c in range(len(Counts)):
                for k in range(len(PrimaryKeys)):
                    if Jobs[AttributeValueIndex][k] == Counts[c]:
                        PlotIndices.append(k)
        else:
            CountsSet = set(TotalJobsPerNode)
            Counts = [c for c in CountsSet]
            Counts.sort(reverse=True)
            for c in range(len(Counts)):
                for k in range(len(PrimaryKeys)):
                    if TotalJobsPerNode[k] == Counts[c]:
                        PlotIndices.append(k)
        
        nbins = min(self.plot_filter_node_number, len(PlotIndices))

        # Sort counts and get self.plot_filter_node_number highest
        FilteredJobs = [[0 for k in range(nbins)] for a in AttributeValues]
        for a in range(len(AttributeValues)):
            for k in range(nbins):
                FilteredJobs[a][k] = Jobs[a][PlotIndices[k]]
        
        # calculate bottom levels in order to enforce stacking
        Bottoms = [[0 for k in range(nbins)] for c in range(
                len(AttributeValues))]
        for cSet in range(1,len(AttributeValues)):
            for cGet in range(0,cSet):
                for k in range(nbins):
                    Bottoms[cSet][k] += FilteredJobs[cGet][k]

        Colors = []
        for i in range(len(AttributeValues)):
            # for list of colormaps see http://wiki.scipy.org/Cookbook/Matplotlib/Show_colormaps
            Colors.append(cm.Spectral(1.0 - i/max(float(len(AttributeValues)-1), 1.0), 1))

        if nbins == 0:
            # break image creation if there are no jobs
            data['error_string'] = "There are no '%s' jobs running" % self.config["groups"]
            data["filename_plot"] = ""
        else:
            xlabels = [0]*nbins
            for i in range(nbins):
                xlabels[i] = PrimaryKeys[PlotIndices[i]]
                if self.use_secondary_key == True:
                    xlabels[i] += ' (' + SecondaryKeys[PlotIndices[i]] + ')'

            pos = np.arange(nbins)+0.5
            fig = self.plt.figure(figsize=(11.0,5.6))
            axis = fig.add_subplot(111)
            p = [axis.barh(pos, FilteredJobs[a], left=Bottoms[a], align='center', 
                    height=0.6, color=Colors[a]) for a in range(len(AttributeValues))]
            fontyAxis = FontProperties()
            fontyAxis.set_size('small')
            axis.set_yticks(pos)
            axis.set_yticklabels(xlabels, fontproperties=fontyAxis)
            
            if self.eval_threshold > -1 and TotalEval >= self.eval_threshold:
                if self.plot_line_warning == 1 and self.eval_threshold_warning >= 0:
                    axis.axvline(TotalEval * self.eval_threshold_warning / 100.0, 
                            color='Yellow',lw=2)
                if self.plot_line_critical == 1 and self.eval_threshold_critical >= 0:
                    axis.axvline(TotalEval * self.eval_threshold_critical / 100.0, 
                            color='Red',lw=3)

            # Prepare legend entries
            p_leg = []
            cat_leg = []
            for i in range(len(p)-1,-1,-1):
               p_leg.append(p[i][0])
               cat_leg.append(AttributeValues[i])

            # Configure plot layout
            fontTitle = FontProperties()
            fontTitle.set_size('medium')
            axis.set_title('24 hours from ' + data['IntervalStart'] + ' to ' \
                    + data['IntervalEnd'] + ' (all times are local)',
                    fontproperties=fontTitle)
            axis.set_position([0.30,0.08,0.53,0.86])
            axis.set_xlabel('Number of Jobs')
            fontLegend = FontProperties()
            fontLegend.set_size('small')
            axis.legend(p_leg, cat_leg, bbox_to_anchor=(1.02, 0.5), loc=6, ncol=1,
                    borderaxespad=0., prop = fontLegend)

            fig.savefig(hf.downloadService.getArchivePath(self.run, 
                    self.instance_name + "_jobs_dist.png"), dpi=91)
            data["filename_plot"] = self.instance_name + "_jobs_dist.png"

        return data

