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

class CMSJobResults(hf.module.ModuleBase):

    config_keys = {
        'source_url': ('URL to XML data source', ''),
        'categories': ('string containing pipe-separated list of categories \
                that are supposed to appear in the specified order', ''),
        'tags': ('pipe-separated list of xml tag names corresponding to the \
                categories specified in categories', ''),
        'eval_categories': ('pipe-separated list of category names that are \
                to be considered in the evaluation', ''),
        'eval_interval': ('number of most recent time intervals that are to \
                be considered in the evaluation', ''),
        'eval_job_threshold': ('number of total jobs above which evaluation \
                is active, below that number module status is set to 1.0', ''),
        'eval_warn_threshold': ('OPERATOR|VALUE where OPERATOR can be < or > \
                to denote whether the threshold is a minimum or maximum and \
                VALUE is a numerical value which denotes the percentage of \
                the sum of the categories specified in eval_categories with \
                respect to the total jobs number', ''),
        'eval_crit_threshold': ('ref eval_warn_threshold', '')
    }
    config_hint = ''
    
    table_columns = ([
        Column("InstanceTitle", TEXT),
        Column("filename_plot", TEXT),
        Column('IntervalStart', TEXT),
        Column('IntervalEnd', TEXT),
        Column('EvaluationStart', TEXT),
        Column('EvaluationEnd', TEXT),
        Column('CurrentHourStart', TEXT),
        Column('CurrentHourEnd', TEXT),
        Column('LastHourStart', TEXT),
        Column('LastHourEnd', TEXT)], ['filename_plot'])

    subtable_columns = {
        'statistics':([
            Column('CatName', TEXT),
            Column('JobsEval', INT),
            Column('JobFracsEval', FLOAT),
            Column('JobsEvalColor', TEXT),
            Column('JobsCurrentHour', INT),
            Column('JobFracsCurrentHour', FLOAT),
            Column('JobsLastHour', INT),
            Column('JobFracsLastHour', FLOAT),
            Column('MinJobs', INT),
            Column('MaxJobs', INT),
            Column('AvgJobs', INT)], [])}

    def prepareAcquisition(self):
        try:
            self.cat_names = self.config['categories'].split('|')
            self.tags = self.config['tags'].split('|')
            self.eval_categories = self.config['eval_categories'].split('|')
            self.eval_interval = int(self.config['eval_interval'])
            self.eval_job_threshold = int(self.config['eval_job_threshold'])
            self.eval_warn_threshold = self.config['eval_warn_threshold'].split('|')
            self.eval_crit_threshold = self.config['eval_crit_threshold'].split('|')
            self.eval_warn_threshold[1] = float(self.eval_warn_threshold[1])
            self.eval_crit_threshold[1] = float(self.eval_crit_threshold[1])
        except KeyError, ex:
            raise hf.exceptions.ConfigError('Required parameter "%s" not specified' % str(e))
        if len(self.cat_names)<>len(self.tags):
            raise hf.exceptions.ConfigError('Different number of categories and \
                    correspondig tags specified')
        self.cat_names.append('total')
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

        StartDates = []
        Jobs = [[0 for t in range(len(rowlist)-1)] for c in range(len(self.cat_names))]
        for i in range(1,len(rowlist)):
            ThisData = {key: 0 for key in self.tags}
            for j in range(len(rowlist[i])):
                if rowlist[i][j].tag == 's_date':
                    StartDates.append(rowlist[i][j].text_content())
                else:
                    if rowlist[i][j].tag in self.tags:
                        ThisData[rowlist[i][j].tag] = rowlist[i][j].text_content()
            for key,value in ThisData.iteritems():
                Jobs[self.tags.index(key)][i-1] = int(value)

        # Calculate derived statistics
        minJobs = [0 for c in range(len(self.cat_names))]
        maxJobs = [0 for c in range(len(self.cat_names))]
        avgJobs = [0 for c in range(len(self.cat_names))]
        JobFractions = [[0 for t in range(len(StartDates))] for c in range(
                len(self.cat_names))]
        AggrJobsEval = 0
        EvalFraction = 0.0
        JobsEval = [0 for c in range(len(self.cat_names))]
        JobFractionsEval = [0 for c in range(len(self.cat_names))]
        JobsEvalColor = ['' for c in range(len(self.cat_names))]
        for t in range(len(StartDates)):
            Jobs[len(self.cat_names)-1][t] = 0
            for c in range(len(self.cat_names)-1):
                Jobs[len(self.cat_names)-1][t] += Jobs[c][t]
        for t in range(len(StartDates)-1, len(StartDates)-1-self.eval_interval, -1):
            for c in range(len(self.cat_names)):
                JobsEval[c] += Jobs[c][t]
        for c in range(len(self.cat_names)):
            minJobs[c] = min(Jobs[c])
            maxJobs[c] = max(Jobs[c])
            for t in range(len(StartDates)):
                if Jobs[len(self.cat_names)-1][t] > 0:
                    JobFractions[c][t] = 100.0 * Jobs[c][t] / float(Jobs[len(
                            self.cat_names)-1][t])
                else:
                    JobFractions[c][t] = 0.0
                avgJobs[c] += Jobs[c][t]
            avgJobs[c] /= len(StartDates)
            if JobsEval[len(self.cat_names)-1] > 0:
                JobFractionsEval[c] = 100.0 * JobsEval[c] / float(JobsEval[len(
                        self.cat_names)-1])
            else:
                JobFractionsEval[c] = 0.0
        
        # Calculate evaluation statistic
        for item in self.eval_categories:
            AggrJobsEval += JobsEval[self.cat_names.index(item)]
        if JobsEval[len(self.cat_names)-1] > 0:
            EvalPercentage = 100.0 * AggrJobsEval / float(JobsEval[len(self.cat_names)-1])
        else:
            EvalPercentage = 0.0

        # Set module status according to evaluation statistic
        data['status'] = 1.0
        if JobsEval[len(self.cat_names)-1] >= self.eval_job_threshold:
            if self.eval_warn_threshold[0] == '<':
                if EvalPercentage <= self.eval_warn_threshold[1]: data['status'] = 0.5
            elif self.eval_warn_threshold[0] == '>':
                if EvalPercentage >= self.eval_warn_threshold[1]: data['status'] = 0.5
            if self.eval_crit_threshold[0] == '<':
                if EvalPercentage <= self.eval_crit_threshold[1]: data['status'] = 0.0
            elif self.eval_crit_threshold[0] == '>':
                if EvalPercentage >= self.eval_crit_threshold[1]: data['status'] = 0.0

        # Color summary subtable cells in column evaluation according to module status
        for item in self.eval_categories:
            if data['status'] == 0.5:
                JobsEvalColor[self.cat_names.index(item)] = 'warning'
            elif data['status'] == 0.0:
                JobsEvalColor[self.cat_names.index(item)] = 'critical'

        # Save subtable data
        for c in range(len(self.cat_names)):
            self.statistics_db_value_list.append({'CatName': self.cat_names[c],
                    'JobsEval': JobsEval[c], 'JobFracsEval': '%.1f' % JobFractionsEval[c],
                    'JobsEvalColor': JobsEvalColor[c],
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

        # Change times from utc to local
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
        data['EvaluationStart'] = StartDates[len(StartDates)-self.eval_interval].split(' ')[1]
        data['EvaluationEnd'] = data['IntervalEnd'].split(' ')[1]

        ######################################################################
        ### Plot data

        #Colors = ['MidnightBlue', 'SteelBlue', 'LightSkyBlue', 'SeaGreen', \
        #        'LightGreen', 'DarkKhaki', 'PaleGoldenrod', 'Khaki', 'LightSalmon', \
        #        'Crimson', 'Maroon']
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
            data['error_string'] = "There are no '%s' jobs running" % self.config["groups"]
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

            # Prepare legend entries
            p_leg = []
            cat_leg = []
            for i in range(len(p)-1,-1,-1):
               p_leg.append(p[i][0])
               cat_leg.append(self.cat_names[i])

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
            axis.set_ylim([0,(max_val+1.0)*1.05])
            fontLegend = FontProperties()
            fontLegend.set_size('small')
            axis.legend(p_leg, cat_leg, bbox_to_anchor=(1.02, 0.5), loc=6, ncol=1,
                    borderaxespad=0., prop = fontLegend)

            fig.savefig(hf.downloadService.getArchivePath(
                    self.run, self.instance_name + "_jobs_dist.png"), dpi=91)
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
