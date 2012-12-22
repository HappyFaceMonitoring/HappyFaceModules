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
from lxml import etree

class PhedexStats(hf.module.ModuleBase):
    config_keys = {
        'phedex_xml': ('URL of the PhEDEx XML file', '')
    }
    config_hint = ''
    
    
    table_columns = [
        Column('startlocaltime', TEXT),
        Column('endlocaltime', TEXT),
        Column('failed_transfers', INT),
    ], []

    subtable_columns = {'details': ([
        Column('site_name', TEXT),
        Column('number', INT),
        Column('origin', TEXT),
        Column('error_message', TEXT),
    ], []),}
    


    def prepareAcquisition(self):

        if 'phedex_xml' not in self.config: raise hf.exceptions.ConfigError('phedex_xml option not set')
        self.phedex_xml = hf.downloadService.addDownload(self.config['phedex_xml'])

        self.details_db_value_list = []

    def extractData(self):
        data = {'source_url': self.phedex_xml.getSourceUrl(),
                'startlocaltime': '',
                'endlocaltime': '',
                'failed_transfers': '',
                'status': 1.0}

        source_tree = etree.parse(open(self.phedex_xml.getTmpPath()))
        root = source_tree.getroot()

        self.startlocaltime = root.get('startlocaltime')
        self.endlocaltime = root.get('endlocaltime')

        data['startlocaltime'] = self.startlocaltime
        data['endlocaltime'] = self.endlocaltime

        failed_transfers = 0

        for fromsite in root.findall('fromsite'):
            for tosite in fromsite:
                for reason in tosite:
                    details_db_values = {}
                    details_db_values['site_name'] = tosite.get('name')
                    details_db_values['number'] = int(reason.get('n'))
                    details_db_values['origin'] = reason.get('origin')
                    details_db_values['error_message'] = reason.text

                    failed_transfers += int(reason.get('n'))
                    self.details_db_value_list.append(details_db_values)

        data['failed_transfers'] = failed_transfers

        return data

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        info_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['info_list'] = map(dict, info_list)

        return data


