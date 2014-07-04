# -*- coding: utf-8 -*-
#
# Copyright 2014 Institut für Experimentelle Kernphysik - Karlsruher Institut für Technologie
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


class PoolCosts(hf.module.ModuleBase):
    config_keys = {
        'source_url': ('Link to Source File', 'http://www-ekp.physik.uni-karlsruhe.de/~happyface/upload/out/pc'),
        'warning_threshold': ('multiplier for status_warning >1.0', '1.95'),
        'critical_threshold': ('multiplier for critical status >1.0', '1.80')
    }
    config_hint = ""

    table_columns = [
	Column('avg_sum', FLOAT),
        Column('avg_cost1', FLOAT),
        Column('avg_cost2', FLOAT)
	], []

    subtable_columns = {
        'details': ([
            Column("pool", TEXT),
            Column("costs_1", FLOAT),
            Column("costs_2", FLOAT)
        ], [])
    }

    def prepareAcquisition(self):

        try:
            self.source_url = self.config['source_url']
            self.warn = self.config['warning_threshold']
            self.crit = self.config['critical_threshold']
        except KeyError, e:
            raise hf.exceptions.ConfigError('Required parameter "%s" not specified' % str(e))
        self.source = hf.downloadService.addDownload(self.config['source_url'])
        self.source_url = self.source.getSourceUrl()
        self.details_list = []

    def extractData(self):
        data = {}
        
        f =  open(self.source.getTmpPath(), 'r')  
	#f = open('/home/marcus/Documents/Hiwi/HappyFace/modules/pc.dat', 'r')
        total_sum = 0.0
        cost1_sum = 0.0
        cost2_sum = 0.0
        length = 0
        for line in f:
            details = {}
            columns = line.split(' ')
            details['pool'] = str(columns[0])
            details['costs_1'] = cost1 = float(columns[1])
            details['costs_2'] = cost2 = float(columns[2])
            self.details_list.append(details)
            total_sum = total_sum + cost1 + cost2
            cost1_sum += cost1
            cost2_sum += cost2
            length += 1
        
        data['status'] = 1.0 #change as soon as an evaluation method is available 
        data['avg_sum'] = total_sum / float(length)
        data['avg_cost1'] = cost1_sum / float(length)
        data['avg_cost2'] = cost2_sum / float(length)
        return data

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute(
            [dict(parent_id=parent_id, **row) for row in self.details_list])
    
    def getTemplateData(self):
	data = hf.module.ModuleBase.getTemplateData(self)
	
        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).execute().fetchall()
        details_list = map(lambda x: dict(x), details_list)
        
        avg_cost = self.dataset['avg_sum']
        warning_cost = float(self.config['warning_threshold']) * avg_cost
	critical_cost = float(self.config['critical_threshold']) * avg_cost
        pools = {}
        pools['crit_pools'] = 0
        pools['warn_pools'] = 0
        pools['avg_pools'] = 0
        pools['low_pools'] = 0
        for i,pool in enumerate(details_list):
	    cost = pool['costs_1'] + pool['costs_2']
	    details_list[i]['sum'] = cost
	    if cost > critical_cost:
		details_list[i]['status'] = 'critical'
		pools['crit_pools'] += 1
	    elif cost > warning_cost:
		details_list[i]['status'] = 'warning'
		pools['warn_pools'] += 1
	    elif cost > avg_cost:
		details_list[i]['status'] = 'ok'
		pools['avg_pools'] += 1
	    else:
		details_list[i]['status'] = 'okay'
		pools['low_pools'] += 1
	data['details'] = details_list
	data['pools'] = pools
	return data
