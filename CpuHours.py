# -*- coding: utf-8 -*-
#
# Copyright 2015 Institut für Experimentelle Kernphysik - Karlsruher Institut für Technologie
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
from sqlalchemy import INT, TEXT, FLOAT, Column, create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker

class SiteStatus(object):
    pass


class CpuHours(hf.module.ModuleBase):
    config_keys = {'source_url' : ('Not used, but filled to avoid warnings', 'http://monitor.ekp.kit.edu/ganglia/')
                  }
    table_columns = [],[]
    subtable_columns = {
		'statistics' : ([
			Column("cloudsite", TEXT),
			Column("cpu_hours", FLOAT),
			Column("unused_cpu_hours", FLOAT),
			Column("usage", FLOAT)], [])
	}

    
    def prepareAcquisition(self):
	# Setting defaults
	self.source_url = self.config["source_url"]
	self.cloudsites = ['bwforcluster', 'condocker', 'ekpsupermachines']	
	# Prepare subtable list for database
        self.statistics_db_value_list = []
    
    def extractData(self):
	# Create data dictionary.
	data = {}
	with open('config/password', 'r') as f:
		engine = create_engine('postgres://hf3_ekplocal:'+ f.read()[:-1] +'@127.0.0.1/hf3_ekplocal', echo=False)

        metadata = MetaData(engine)
        subtable = Table('sub_ht_condor_site_status_statistics', metadata, autoload=True)
        mapper(SiteStatus, subtable)

        Session = sessionmaker(bind=engine)
        session = Session()

        res = session.query(SiteStatus).all()
        max_parent_id = res[-1].parent_id
	past_hours = 24
        num_of_parent_ids = past_hours * 4
        for cloudsite in self.cloudsites:
	    cloudsite_dict = {'cloudsite': cloudsite, 'cpu_hours': 0., 'unused_cpu_hours': 0.}
	    for entry in reversed(res):
	        if entry.cloudsite == cloudsite and entry.parent_id > max_parent_id - num_of_parent_ids:
		    cloudsite_dict['cpu_hours'] += 0.25 * entry.busy
		    cloudsite_dict['unused_cpu_hours'] += 0.25 * entry.idle
	    if cloudsite_dict['unused_cpu_hours'] + cloudsite_dict['cpu_hours'] == 0:
		cloudsite_dict['usage'] = 0.00
	    else:
	        cloudsite_dict['usage'] = round(cloudsite_dict['cpu_hours'] *100 / (cloudsite_dict['unused_cpu_hours'] + cloudsite_dict['cpu_hours']),2)
	    self.statistics_db_value_list.append(cloudsite_dict)
	return data

    def fillSubtables(self, parent_id):
                self.subtables['statistics'].insert().execute([dict(parent_id=parent_id, **row) for row in self.statistics_db_value_list])

    def getTemplateData(self):

                data = hf.module.ModuleBase.getTemplateData(self)
                statistics_list = self.subtables['statistics'].select().\
                        where(self.subtables['statistics'].c.parent_id == self.dataset['id']).execute().fetchall()
                data["statistics"] = map(dict, statistics_list)
                return data

