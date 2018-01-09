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

import datetime
import hf
import json
from sqlalchemy import TEXT, INT, FLOAT, Column


class CMSDatasetPopularity(hf.module.ModuleBase):
    config_keys = {
        'source_url': ('set url of source', '|<native>|https://cmsweb.cern.ch/popdb/popularity/DSStatInTimeWindow/?sitename=T2_DE_DESY'),
        'n_datasets': ('number of datasets to show', 10),
        'usage': ('most or least: show most/least used', 'most'),
        'days': ('timespan [days] to consider', 10),
    }
    config_hint = ''

    table_columns = [Column('timespan', TEXT), Column('site', TEXT)], []

    subtable_columns = {
        'details': ([Column('collname', TEXT),
                     Column('nacc', INT),
                     Column('totcpu', INT),
                     Column('nusers', INT),
        ],[])
    }


    def prepareAcquisition(self):
        self.n_datasets = int(self.config['n_datasets'])
        self.usage = self.config['usage']
        self.days = int(self.config['days'])
        self.timespan = self._getTimespan(self.days)

        source_url = self.config['source_url']
        source_url += '&tstart=%s&tstop=%s' % self.timespan
        self.source = hf.downloadService.addDownload(source_url)
        self.source_url = self.source.getSourceUrl()

        self.rows = []

    def extractData(self):
        with open(self.source.getTmpPath()) as in_f:
            data = json.load(in_f)

        site = data['SITENAME']
        data = data['DATA']
        reverse = (self.usage == 'most')
        data = sorted(data, key=lambda item: item['NACC'], reverse=reverse)

        for item in data[:self.n_datasets]:
            self.rows.append({
                'collname': item['COLLNAME'],
                'nacc': item['NACC'],
                'totcpu': item['TOTCPU'],
                'nusers': item['NUSERS'],
                })

        timespan = 'from %s to %s' % self.timespan
        timespan += ' (%d days)' % self.days
        return {'status': 1., 'site': site, 'timespan': timespan}

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.rows])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id == self.dataset['id']).execute().fetchall()
        details_list = map(dict, details_list)

        data['details'] = details_list
        return data

    @staticmethod
    def _getTimespan(days):
        delta = datetime.timedelta(days=days)
        stop = datetime.date.today()
        start = stop - delta

        return start.strftime("%Y-%m-%d"), stop.strftime("%Y-%m-%d")
