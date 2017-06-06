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
from  lxml.html import parse
import StringIO
from sqlalchemy import TEXT, INT, FLOAT, Column
import numpy as np
from datetime import datetime
import pytz

state_colors = {
    'cleanup': '#909090',
    'production':   '#009933',
    'logcollect':   '#9D5CDE',
    'merge':      '#AA0000',
    'processing': '#5CADFF'
}

def getcolor(label):
    for k in state_colors.keys():
        if k in label.lower():
            return state_colors[k]
    return None  # None makes way for the Colors defined during plotting



class CMSJobs(hf.module.ModuleBase):
    config_keys = {
        'source_url': ('URL to XML data source', ''),
        'categories': ('string containing pipe separated list of categories \
                that are supposed to appear in the specified order', ''),
        'category_tag': ('tag containing category of \
                datapoint within an xml item', ''),
        'data_tag': ('tag containing numerical data to be used \
                within an xml item', ''),
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
            self.cat_names = filter(lambda x: x != '', self.config['categories'].split('|'))
            self.pledge = self.config['pledge']
        except KeyError, ex:
            raise hf.exceptions.ConfigError('Required parameter "%s" not specified' % str(ex))
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
        import matplotlib.pyplot as plt
        from matplotlib.font_manager import FontProperties
        self.plt = plt

        data = {}
        data["filename_plot"] = ""
        webpage = open(self.source.getTmpPath())
        strwebpage = webpage.read()
        tree = parse(StringIO.StringIO(strwebpage))

        # Function to convert raw time data given in UTC to local time zone
        def ChangeTimeZone(TimeStringIn, InFormatString, OutFormatString):
            Date = datetime.strptime(TimeStringIn, InFormatString).replace(
                    tzinfo=pytz.utc).astimezone(pytz.timezone('Europe/Berlin'))
            return(Date.strftime(OutFormatString))

        # Get lists for startdates, categories, data
        timeseries = {}
        for entry in tree.findall('.//jobs/item'):
            start_date = entry.find('s_date').text_content()
            cat = entry.find(str(self.config['category_tag'])).text_content()
            value = entry.find(str(self.config['data_tag'])).text_content()
            start_date = ChangeTimeZone(start_date, "%d-%b-%y %H:%M:%S", "%Y-%m-%d %H:%M:%S")
            timeseries.setdefault(start_date, {})[cat] = int(value)
            if cat not in self.cat_names:
                self.cat_names.append(cat)

        # Get categories and add them to the category list derived from
        # the config file if they are not already listed there
        category_overview = {}
        for tp in timeseries:
            timeseries[tp]['total'] = sum(timeseries[tp].values())
            for cat in (self.cat_names + ['total']):
                category_overview.setdefault(cat, {})[tp] = timeseries[tp].get(cat, 0)

        time_points = sorted(timeseries)
        current_hour = '0 0'
        last_hour = '0 0'
        if len(time_points) > 1:
            current_hour = time_points[-1]
            last_hour = time_points[-2]

        for cat in self.cat_names:
            jobs_current = timeseries[current_hour].get(cat, 0)
            jobs_last = timeseries[last_hour].get(cat, 0)
            frac_current = jobs_current / float(max(1, timeseries[current_hour].get('total', 0)))
            frac_last = jobs_last / float(max(1, timeseries[last_hour].get('total', 0)))
            job_list = category_overview.get(cat, {}).values()
            cat_data = {'CatName': cat,
                'JobsCurrentHour': jobs_current,
                'JobFracsCurrentHour': '%.1f' % (100.0 * frac_current),
                'JobsLastHour': jobs_last,
                'JobFracsLastHour': '%.1f' % (100.0 * frac_last),
                'MinJobs': min(job_list), 'MaxJobs': max(job_list),
                'AvgJobs': sum(job_list) / float(max(1, len(timeseries)))}
            self.statistics_db_value_list.append(cat_data)

        data['InstanceTitle'] = self.config['name']
        data['IntervalStart'] = ChangeTimeZone(tree.find(".//start").text_content(
                ).split(".")[0], "%Y-%m-%d %H:%M:%S", "%d-%b-%y %H:%M:%S")
        data['IntervalEnd'] = ChangeTimeZone(tree.find(".//end").text_content(
                ).split(".")[0], "%Y-%m-%d %H:%M:%S", "%d-%b-%y %H:%M:%S")
        data['CurrentHourStart'] = current_hour.split(' ')[1]
        data['CurrentHourEnd'] = data['IntervalEnd'].split(' ')[1]
        data['LastHourStart'] = last_hour.split(' ')[1]
        data['LastHourEnd'] = data['CurrentHourStart']
        data['status'] = 1.0
        if self.pledge_mode == 1:
            yhlinetext = tree.find(".//{}".format(self.pledge)).text_content()
            try:
                yhline = int(yhlinetext)
            except Exception:
                yhline = 0
        elif self.pledge_mode == 2:
            yhline = self.pledge

        ################################################################
        ### Plot data
        
        nbins = len(time_points)
        if nbins == 0:
            # break image creation if there are no jobs
            data['error_string'] = "No plot is generated because data source contains no jobs to be displayed."
            data["filename_plot"] = ""
        else:
            ind = np.arange(nbins)   # the x locations for the groups
            width = 1.00   # the width of the bars: can also be len(x) sequence
            max_val = max(category_overview['total'].values())
            xlabels = [0]*nbins
            for i in range(0, nbins):
                if i % 2 == 0:
                    DateLabel = time_points[i].split(' ')[0]
                    TimeLabel = time_points[i].split(' ')[1].split(':')
                    xlabels[i] = DateLabel + '\n' + TimeLabel[0] + ':' + TimeLabel[1]
                else:
                    xlabels[i] = ''

            # calculate bottom levels in order to enforce stacking
            cat_bottoms = [[0] * len(time_points)] * len(self.cat_names)
            for cat_idx, cat in enumerate(self.cat_names):
                if cat_idx > 0:
                    cat_bottoms[cat_idx] = list(cat_bottoms[cat_idx - 1])
                for t_idx, tp in enumerate(time_points):
                    cat_bottoms[cat_idx][t_idx] += category_overview.get(cat, {}).get(tp, 0)

            # Create figure and plot job numbers of the different categories
            fig = self.plt.figure(figsize=(10,5.8))
            axis = fig.add_subplot(111)
            p = []
            for cat_idx, cat in enumerate(self.cat_names):
                values = []
                for tp in time_points:
                    values.append(category_overview[cat][tp])
                if cat_idx > 0:
                    p.append(axis.bar(ind, values, width, color=getcolor(cat),
                        bottom=cat_bottoms[cat_idx - 1]))
                else:
                    p.append(axis.bar(ind, values, width, color=getcolor(cat)))
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
            axis.set_position([0.10,0.20,0.68,0.72])
            axis.set_ylabel('Number of Jobs')
            axis.set_xticks(ind + width / 2.0)
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
