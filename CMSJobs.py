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

class CMSJobs(hf.module.ModuleBase):

    config_keys = {
        'source_url': ('URL to XML data source', ''),
        'start_date_tag_index': ('index of tag containing start date and time \
                of datapoint within an xml item', ''),
        'category_tag_index': ('index of tag containing category of \
                datapoint within an xml item', ''),
        'data_tag_index': ('index of tag containing numerical data to be used \
                within an xml item', ''),
        'categories': ('string containing pipe separated list of categories \
                that are supposed to appear in the specified order', ''),
        'pledge': ('either numerical value or tag name that contains y-value of \
                horizontal line to be drawn across the plot, leave blank if no \
                line is to be drawn, if you want to specify a tag name then put \
                it in angle brackets (i.e. <TAGNAME>)', '')
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
            self.start_date_tag_index = int(self.config['start_date_tag_index'])
            self.category_tag_index = int(self.config['category_tag_index'])
            self.data_tag_index = int(self.config['data_tag_index'])
            self.cat_names = self.config['categories'].split('|')
            self.pledge = self.config['pledge']
        except KeyError, ex:
            raise hf.exceptions.ConfigError('Required parameter "%s" not specified' % str(e))
        if self.pledge == '':
            self.pledge_mode = 0 # no pledge given -> ignore
        elif '<' in self.pledge:
            self.pledge_mode = 1 # pledge to be read from xml file, tag name given in config file
            self.pledge = self.pledge.replace('<','').replace('>','')
        else:
            self.pledge_mode = 2 # pledge value itself given in config file
            self.pledge = int(self.pledge)
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
        webpage = open(self.source.getTmpPath())
        strwebpage = webpage.read()
        tree = lxml.html.parse(StringIO.StringIO(strwebpage))
        rowlist = tree.findall(".//item")

        # Get different StartDates
        StartDates = []
        for i in range(1,len(rowlist)):
            StartDate = rowlist[i][self.start_date_tag_index].text_content()
            if not StartDate in StartDates:
                StartDates.append(StartDate)

        # Get categories and add them to the category list derived from
        # the config file if they are not already listed there
        for i in range(1,len(rowlist)):
            Category = rowlist[i][self.category_tag_index].text_content()
            if not Category in self.cat_names:
                self.cat_names.append(Category)
        self.cat_names.append('total')

        # Initialize nested job list: Jobs[c][t]
        Jobs = [[0 for t in range(len(StartDates))] for c in range(len(self.cat_names))]
        for i in range(1,len(rowlist)):
            iStartDate = rowlist[i][self.start_date_tag_index].text_content()
            iCategory = rowlist[i][self.category_tag_index].text_content()
            iData = int(rowlist[i][self.data_tag_index].text_content())
            t = StartDates.index(iStartDate)
            c = self.cat_names.index(iCategory)
            Jobs[c][t] = iData

        # Calculate derived statistics
        minJobs = [0 for c in range(len(self.cat_names))]
        maxJobs = [0 for c in range(len(self.cat_names))]
        avgJobs = [0 for c in range(len(self.cat_names))]
        JobFractions = [[0 for t in range(len(StartDates))] for c in range(len(self.cat_names))]
        for t in range(len(StartDates)):
            Jobs[len(self.cat_names)-1][t] = 0
            for c in range(len(self.cat_names)-1):
                Jobs[len(self.cat_names)-1][t] += Jobs[c][t]
        for c in range(len(self.cat_names)):
            minJobs[c] = min(Jobs[c])
            maxJobs[c] = max(Jobs[c])
            for t in range(len(StartDates)):
                if Jobs[len(self.cat_names)-1][t] > 0:
                    JobFractions[c][t] = 100.0 * Jobs[c][t] / float(Jobs[len(self.cat_names)-1][t])
                else:
                    JobFractions[c][t] = 0.0
                avgJobs[c] += Jobs[c][t]
            avgJobs[c] /= len(StartDates)
            self.statistics_db_value_list.append({'CatName': self.cat_names[c],
                    'JobsCurrentHour': Jobs[c][len(StartDates)-1],
                    'JobFracsCurrentHour': '%.1f' % JobFractions[c][len(StartDates)-1],
                    'JobsLastHour': Jobs[c][len(StartDates)-2],
                    'JobFracsLastHour': '%.1f' % JobFractions[c][len(StartDates)-2],
                    'MinJobs': minJobs[c], 'MaxJobs': maxJobs[c],
                    'AvgJobs': avgJobs[c]})

        # Function to convert raw time data given in UTC to local time zone
        def ChangeTimeZone(TimeStringIn, InFormatString, OutFormatString):
            Date = datetime.strptime(TimeStringIn, InFormatString).replace(
                    tzinfo=pytz.utc).astimezone(pytz.timezone('Europe/Berlin'))
            return(Date.strftime(OutFormatString))

        # Change times to local
        StartDatesRaw = StartDates[:]
        for t in range(len(StartDates)):
            StartDates[t] = ChangeTimeZone(StartDates[t], "%d-%b-%y %H:%M:%S",
                    "%d-%b-%y %H:%M:%S")

        data['InstanceTitle'] = self.config['name']
        data['IntervalStart'] = ChangeTimeZone(tree.find(".//start").text_content(
                ).split(".")[0], "%Y-%m-%d %H:%M:%S", "%d-%b-%y %H:%M:%S")
        data['IntervalEnd'] = ChangeTimeZone(tree.find(".//end").text_content(
                ).split(".")[0], "%Y-%m-%d %H:%M:%S", "%d-%b-%y %H:%M:%S")
        data['CurrentHourStart'] = StartDates[len(StartDates)-1].split(' ')[1]
        data['CurrentHourEnd'] = data['IntervalEnd'].split(' ')[1]
        data['LastHourStart'] = StartDates[len(StartDates)-2].split(' ')[1]
        data['LastHourEnd'] = StartDates[len(StartDates)-1].split(' ')[1]
        data['status'] = 1.0
        if self.pledge_mode == 1:
            yhline = int(tree.find(".//{}".format(self.pledge)).text_content())
        elif self.pledge_mode == 2:
            yhline = self.pledge

        ################################################################
        ### Plot data

        Colors = []
        for i in range(len(self.cat_names)-1):
            # for list of colormaps see http://wiki.scipy.org/Cookbook/Matplotlib/Show_colormaps
            Colors.append(cm.Spectral(1.0 - i/max(float(len(self.cat_names)-2),1.0), 1))
            # Colors.append(cm.spectral((i+1)/float(len(self.cat_names)-0), 1))
            # Colors.append(cm.jet(i/float(len(self.cat_names)-2), 1))
            # Colors.append(cm.gist_earth((i+1)/float(len(self.cat_names)-1), 1))
            # Colors.append(cm.RdBu(1.0 - i/float(len(self.cat_names)-2), 1))
            # Colors.append(cm.YlGnBu(1.0 - i/float(len(self.cat_names)-2), 1))

        nbins = len(StartDates)
        if nbins == 0:
            # break image creation if there are no jobs
            data['error_string'] = "No plot is generated because data source contains no jobs to be displayed."
            data["filename_plot"] = ""
        else:
            ind = np.arange(nbins)   # the x locations for the groups
            width = 1.00   # the width of the bars: can also be len(x) sequence
            max_val = maxJobs[len(self.cat_names)-1]
            xlabels = [0]*nbins
            for i in range(0,nbins):
                if i % 2 == 0:
                    DateLabel = StartDates[i].split(' ')[0].split('-')
                    TimeLabel = StartDates[i].split(' ')[1].split(':')
                    xlabels[i] = DateLabel[0] + '-' + DateLabel[1] + '\n' + \
                            TimeLabel[0] + ':' + TimeLabel[1]
                else:
                    xlabels[i] = ''

            # calculate bottom levels in order to enforce stacking
            cat_bottoms = [[0 for t in range(len(StartDates))] for c in range(
                    len(self.cat_names)-1)]
            for cSet in range(1,len(self.cat_names)-1):
                for cGet in range(0,cSet):
                    for t in range(len(StartDates)):
                        cat_bottoms[cSet][t] += Jobs[cGet][t]

            # Create figure and plot job numbers of the different categories
            fig = self.plt.figure(figsize=(10,5.8))
            axis = fig.add_subplot(111)
            p = [axis.bar(ind, Jobs[c], width, color=Colors[c], bottom=cat_bottoms[c]
                    ) for c in range(len(self.cat_names)-1)]
            if self.pledge_mode > 0:
                l = axis.axhline(y=yhline, linewidth=3.0, color='Black')
            else:
                yhline = 0

            # Prepare legend entries
            p_leg = []
            cat_leg = []
            for i in range(len(p)-1,-1,-1):
               p_leg.append(p[i][0])
               cat_leg.append(self.cat_names[i])
            if self.pledge_mode > 0:
                p_leg.append(l)
                cat_leg.append('pledge: {} jobs'.format(yhline))

            # Configure plot layout
            fontTitle = FontProperties()
            fontTitle.set_size('medium')
            axis.set_title('24 hours from ' + data['IntervalStart'] + ' to ' \
                    + data['IntervalEnd'] + ' (all times are local)',
                    fontproperties=fontTitle)
            axis.set_position([0.10,0.12,0.68,0.82])
            axis.set_ylabel('Number of Jobs')
            axis.set_xticks(ind + 0.0 * width / 2.0)
            axis.set_xticklabels(xlabels, rotation='vertical')
            axis.set_autoscaley_on(False)
            axis.set_ylim([0,(max(max_val, yhline)+1.0)*1.05])
            fontLegend = FontProperties()
            fontLegend.set_size('small')
            axis.legend(p_leg, cat_leg, bbox_to_anchor=(1.02, 0.5), loc=6, ncol=1,
                    borderaxespad=0., prop = fontLegend)

            fig.savefig(hf.downloadService.getArchivePath(self.run, 
                    self.instance_name + "_jobs_dist.png"), dpi=91)
            data["filename_plot"] = self.instance_name + "_jobs_dist.png"

        return data

    def fillSubtables(self, parent_id):
        self.subtables['statistics'].insert().execute(
                [dict(parent_id=parent_id, **row) for row in self.statistics_db_value_list])
    
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['statistics'].select().where(
                self.subtables['statistics'].c.parent_id==self.dataset['id']
                ).execute().fetchall()
        data['statistics'] = map(dict, details_list)
        return data
