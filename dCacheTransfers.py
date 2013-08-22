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
from sqlalchemy import *
from lxml.html import parse
from string import strip
import math
class dCacheTransfers(hf.module.ModuleBase):
    config_keys = {
        'speed_warning_limit': ('Warn if the speed is less than n KiB/s', '250'),
        'speed_critical_limit': ('Display error if the speed is less than n KiB/s', '150'),
        'time_warning_limit': ('Warn if age of a transfer is older than n hours', '72'),
        'time_critical_limit': ('Display error if age of a transfer is older than n hours', '96'),
        'rating_ratio': ('Calculate rating of the specified fraction of transfers has errors or warnings', '0.1'),
        'rating_threshold': ('Rate only if there are more than n transfers', '10'),
        'source_url': ('', ''),
    }
    config_hint = ''

    table_columns = [
        Column('speed_average', INT),
        Column('speed_stdev', INT),
        Column('below_speed_warning_limit', INT),
        Column('below_speed_critical_limit', INT),
        Column('exceed_time_warning_limit', INT),
        Column('exceed_time_critical_limit', INT),
        Column('total_transfers', INT),
        Column('warning_transfers', INT),
        Column('critical_transfers', INT)
    ], []

    subtable_columns = {'details': ([
        Column('protocol', TEXT),
        Column('pnfsid', TEXT),
        Column('pool', TEXT),
        Column('host', TEXT),
        Column('status_text', TEXT),
        Column('since', INT),
        Column('transferred', FLOAT),
        Column('speed', INT),
        Column('status', FLOAT)
    ], [])}


    def prepareAcquisition(self):

        try:
            self.speed_warning_limit = int(self.config['speed_warning_limit'])
            self.speed_critical_limit = int(self.config['speed_critical_limit'])
            self.time_warning_limit = int(self.config['time_warning_limit'])
            self.time_critical_limit = int(self.config['time_critical_limit'])
            self.rating_ratio = float(self.config['rating_ratio'])
            self.rating_threshold = int(self.config['rating_threshold']) 
        except KeyError, ex:
            raise hf.exceptions.ConfigError('Required parameter "%s" not specified' % str(e))
        if 'source_url' not in self.config: raise hf.exceptions.ConfigError('No source file')
        self.source = hf.downloadService.addDownload(self.config['source_url'])
        self.source_url = self.source.getSourceUrl()
        self.details_db_value_list = []

    def extractData(self):
        data = {}
        data['below_speed_warning_limit'] = 0
        data['below_speed_critical_limit'] = 0
        data['exceed_time_warning_limit'] = 0
        data['exceed_time_critical_limit'] = 0
        data['total_transfers'] = 0
        data['warning_transfers'] = 0
        data['critical_transfers'] = 0
        root = parse(self.source.getTmpPath()).getroot()
        root = root.findall('.//tbody')[0]
        speed_sum = 0
        for line in root.findall('.//tr'):
            try:
	      tds = line.findall('.//td')
	      appender = {}
	      appender['protocol'] = tds[2].findall('.//span')[0].text
	      appender['pnfsid'] = tds[5].findall('.//span')[0].text
	      appender['pool'] = tds[6].findall('.//span')[0].text
	      appender['host'] = 'empty'
	      appender['status_text'] = tds[7].findall('.//span')[0].text
	      e_time = map(int, map(strip, tds[8].findall('.//span')[0].text.split(':')))
	      appender['since'] = e_time[0] * 3600 + e_time[1] * 60 + e_time[2]
	      appender['transferred'] = float(tds[10].findall('.//span')[0].text) * 1000 / 1024.0 / 1024.0 / 1024.0
	      try:
                  appender['speed'] = float(tds[11].findall('.//span')[0].text) * 1000.0 / 1024.0
              except ValueError:
                  appender['speed'] = 0.0
	      data['total_transfers'] += 1
	      speed_sum += appender['speed']
	      if int(appender['speed']) <= self.speed_critical_limit:
		  appender['status'] = 0.0
		  data['below_speed_critical_limit'] += 1
	      elif int(appender['speed']) <= self.speed_warning_limit:
		  appender['status'] = 0.5
		  data['below_speed_warning_limit'] += 1
	      else:
		  appender['status'] = 1.0
		  
	      if appender['since'] >= (self.time_critical_limit * 3600) and appender['status'] != 0.0:
		  appender['status'] = 0.0
		  data['exceed_time_critical_limit'] += 1
	      elif appender['since'] >= (self.time_warning_limit * 3600) and appender['status'] == 1.0:
		  appender['status'] = 0.5
		  data['exceed_time_warning_limit'] += 1
	      self.details_db_value_list.append(appender)
            except IndexError:
                continue

        data['warning_transfers'] = data['below_speed_warning_limit'] + data['exceed_time_warning_limit']
        data['critical_transfers'] = data['below_speed_critical_limit'] + data['exceed_time_critical_limit']

        if data['total_transfers']<>0:
	  data['speed_average'] = int(speed_sum / data['total_transfers'])
	else:
	  data['speed_average'] = 0
        speed_avg = data['speed_average']
        speed_delta = 0
        total_jobs = data['total_transfers']
        for i, item in enumerate(self.details_db_value_list):
            speed_delta += 1.0 /((float(total_jobs) - 1.0) * float(total_jobs)) * (float(item['speed']) - float(speed_avg)) ** 2
        data['speed_stdev'] = int(math.sqrt(speed_delta))

        if float(data['warning_transfers']) / data['total_transfers'] >= self.rating_ratio and data['total_transfers'] >= self.rating_threshold:
            data['status'] = 0.5
        elif float(data['warning_transfers'] + data['critical_transfers']) / data['total_transfers'] >= self.rating_ratio and data['total_transfers'] >= self.rating_threshold:
            data['status'] = 0.5
        elif float(data['critical_transfers']) / data['total_transfers'] >= self.rating_ratio and data['total_transfers'] >= self.rating_threshold:
            data['status'] = 0.0
        else:
            data['status'] = 1.0
        return data

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['details'] = map(dict, details_list)

        for i,item in enumerate(data['details']):
            if item['status'] == 1.0:
                data['details'][i]['status'] = 'ok'
            elif item['status'] == 0.5:
                data['details'][i]['status'] = 'warning'
            else:
                data['details'][i]['status'] = 'critical'
            store = item['since']
            data['details'][i]['since'] = str('%02i' %int(store / (24 * 3600))) + ':' + str('%02i' %int((store % (24 * 3600)) / (3600))) + ':' + str('%02i' %int(((store % (24 * 3600)) % (3600) / (60)))) + ':' + str('%02i' %int(((store % (24 * 3600)) % (3600)) % (60)))

        return data
