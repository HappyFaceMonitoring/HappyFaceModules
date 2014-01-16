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

import hf, logging
from sqlalchemy import *
from lxml.html import parse
from string import strip
import datetime
import re

class dCacheDatasetRestoreLazyGoe(hf.module.ModuleBase):
    config_keys = {
        'source': ('URL of the dCache Dataset Restore Monitor (Lazy)', ''),
        'stage_max_retry': ('Retry limit', '2'),
        'stage_max_time': ('Time limit (in hours)', '48'),
#       'details_cutoff': ('Max. number of details', '100'),
        'limit_warning': ('Warning limit', '5'),
        'limit_critical': ('Critical limit', '10')
    }
    config_hint = ''

    table_columns = [
        Column('total', INT),
        Column('total_problem', INT),
        Column('status_pool2pool', INT),
        Column('status_staging', INT),
        Column('status_waiting', INT),
        Column('status_suspended', INT),
        Column('status_unknown', INT),
        Column('time_limit', INT),
        Column('retry_limit', INT),
        Column('hit_retry', INT),
        Column('hit_time', INT),
    ], []

    subtable_columns = {'details': ([
        Column('pnfs', TEXT),
        Column('path', TEXT),
        Column('started_full', TEXT),
        Column('retries', INT),
        Column('status_short', TEXT),
    ], [])}


    def prepareAcquisition(self):

        self.source = hf.downloadService.addDownload(self.config['source'])
        self.source_url = self.source.getSourceUrl()
        self.stage_max_retry = int(self.config['stage_max_retry'])
        self.stage_max_time = int(self.config['stage_max_time'])
        self.limit_warning = int(self.config['limit_warning'])
        self.limit_critical = int(self.config['limit_critical'])

        try:
            self.details_cutoff = int(self.config['details_cutoff'])
        except hf.ConfigError:
            self.details_cutoff = 0

        self.statusTagsOK = ['Pool2Pool','Staging']
        self.statusTagsFail = ['Waiting','Suspended','Unknown']

        self.total = 0
        self.total_problem = 0
        self.hit_retry = 0
        self.hit_time = 0

        self.status = 1.0

        self.details_db_value_list = []

    def extractData(self):

        data = {'time_limit': self.stage_max_time,
                'retry_limit': self.stage_max_retry,
                'status': self.status}
        self.stage_max_time = datetime.timedelta(hours = self.stage_max_time)
        count = {}
        critical = 0
        expired = 0
        retried = 0
        for tag in (self.statusTagsOK + self.statusTagsFail):
          count[tag] = 0
        
        source_tree = parse(open(self.source.getTmpPath()))
        root = source_tree.getroot()
        root = root.findall('.//tbody')[0].findall('.//tr')
        stage_requests = []
        current_time = datetime.datetime.today()

        # parse html
        for tr in root:
          tds = tr.findall('.//td')
          pnfs = tds[0].text
          started = tds[3].text
          retries = int(tds[5].text)
          status = tds[6].text
          stat_name = [ x for x in (self.statusTagsOK + self.statusTagsFail) if x in status]
          stat_name = stat_name[0]
          count[stat_name] += 1
          started = map(strip, started.split())
          month_day = map(int, map(strip, started[0].split('.')))
          hour_min_sec = map(int, map(strip, started[1].split(':')))
          job_time = datetime.datetime(current_time.year, month_day[0], month_day[1], hour_min_sec[0], hour_min_sec[1], hour_min_sec[2])
          if (current_time - job_time) < datetime.timedelta(microseconds = 0):
            job_time = datetime.datetime(current_time.year - 1, month_day[0], month_day[1], hour_min_sec[0], hour_min_sec[1], hour_min_sec[2])
          if (current_time - job_time) > self.stage_max_time:
            expired += 1
            stat_name += '  Expired'
          if retries > self.stage_max_retry:
            retried += 1
            stat_name += '  Tried'
          
          bools = [x in stat_name for x in (self.statusTagsFail + ['Expired', 'Tried'])]
          if True in bools:
            critical += 1
          info = {'pnfs': pnfs, 'started_full': job_time.isoformat(' '), 'status_short': stat_name, 'retries':retries, 'path': 'empty'}
          self.details_db_value_list.append(info)
        
        
        for tag in (self.statusTagsOK + self.statusTagsFail):
          data['status_'+tag.lower()] = count[tag]
        data['hit_retry'] = retried
        data['hit_time'] = expired
        data['total_problem'] = critical
        if critical >= self.limit_warning:
            self.status = 0.5
        if critical >= self.limit_critical:
            self.status = 0.0
        data['status'] = self.status
        data['total'] = len(self.details_db_value_list)
        return data

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        info_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).execute().fetchall()
        all_requests_list = map(dict, info_list)
        self.statusTagsOK = ['Pool2Pool','Staging']
        self.statusTagsFail = ['Waiting','Suspended','Unknown']
        for x in (self.statusTagsFail + self.statusTagsOK + ['Expired','Tried']):
          data[x] = []
          
        for request in all_requests_list:
          for tag in (self.statusTagsFail + self.statusTagsOK + ['Expired','Tried']):
            if tag in request['status_short']:
              data[tag].append(request)
        return data
