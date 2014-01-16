# Copyright 2013 II. Physikalisches Institut - Georg-August-Universitaet Goettingen
# Author: Christian Georg Wehrberger (christian@wehrberger.de)
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
from sqlalchemy import *
from lxml import etree
import re
from datetime import datetime
from datetime import timedelta
from BeautifulSoup import BeautifulSoup

class G_Stat(hf.module.ModuleBase):
    config_keys = {
        'source_html': ('HTML Source File', 'both||http://gstat2.grid.sinica.edu.tw/gstat/site/GoeGrid/bdii_site/bdii.goegrid.gwdg.de/all/'),
    }

    #config_hint = ''

    table_columns = [
        Column('source_html', TEXT),
    ], []

    subtable_columns = {
        'details_table': ([
        Column('bdii_hostname', TEXT),
        Column('service_name', TEXT),
        Column('current_state', TEXT), 
        Column('information', TEXT),
        Column('last_check', DATETIME),
        Column('error_info', TEXT)
    ], [])}

    def prepareAcquisition(self):
        # get urls from config and queue them for downloading
        self.gstat_html = hf.downloadService.addDownload(self.config['source_html'])
                
        self.details_table_db_value_list = []

    def extractData(self):
        data = {
            'source_html': self.config['source_html'],
            'status': 1
            }
     
        data_gstat_html = open(self.gstat_html.getTmpPath()).read().replace('\n',' ').replace('\r',' ')

        if len(data_gstat_html) < 2:
            data['status'] = -1

        soup = BeautifulSoup(data_gstat_html)
          
        # check whether there is a table
        if False:
            print('Error while parsing GStat HTML page: Could not find expected table.')
            data['status'] = -1
        else:
            for table in soup.findAll('table'):
                for tbody in table.findAll('tbody'):
                    detail = {}
                    for index_row, row in enumerate(tbody.findAll('tr')):
                        if index_row % 2 == 0: # regular row
                            for index_col, col in enumerate(row.findAll('td')):
                                if index_col == 0:
                                    continue
                                elif index_col == 1:
                                    detail['bdii_hostname'] = str(col.string).strip()
                                elif index_col == 2:
                                    detail['service_name'] = str(col.string).strip()
                                elif index_col == 3:
                                    for span in col.findAll('span'):
                                         detail['current_state'] = str(span.string).strip()
                                elif index_col == 4:
                                    detail['information'] = str(col.string).strip()
                                elif index_col == 5:
                                    for script in col.findAll('script'):
                                        time_seconds = re.findall(r'[0-9]+', str(script.string))
                                        detail['last_check'] = datetime.fromtimestamp(int(time_seconds[0]))
                            self.details_table_db_value_list.append({})
                            self.details_table_db_value_list[index_row//2] = detail
                        else: # hidden row for error info
                            for col in row.findAll('td'):
                                for script in col.findAll('script'):
                                    detail = {}
                                    error_info = str(script.string[script.string.find('(\'')+2:script.string.find(';')-2])
                                    #error_info = error_info.replace('%','%%')
                                    error_info = error_info.replace('\\n','<br>')
                                    detail['error_info'] = error_info
                                    #print detail
                                    #print index_row
                                    self.details_table_db_value_list[index_row//2]['error_info'] = error_info
        
        #print self.details_table_db_value_list
        for detail in self.details_table_db_value_list:
            if detail['current_state'].lower() == 'critical':
                data['status'] = min(data['status'],0)
            elif detail['current_state'].lower() == 'warning':
                data['status'] = min(data['status'],0.5)
            elif detail['current_state'].lower() == 'ok':
                data['status'] = min(data['status'],1)
            else:
                data['status'] = min(data['status'],0)
                

        return data

    def fillSubtables(self, parent_id):
 
        self.subtables['details_table'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_table_db_value_list])
   
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details = self.subtables['details_table'].select().where(self.subtables['details_table'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['details_gstat'] = map(dict, details)

        return data

