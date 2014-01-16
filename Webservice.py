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
import hf.database
from sqlalchemy import *
import json

class Webservice(hf.module.ModuleBase):
    config_keys = {
        'database_structure_file': ('JSON structure file for the database', 'local||http://127.0.0.1/webservice/db_structure.json'),
    }

    config_hint = ''

    table_columns = [
        Column('database_structure_file', TEXT),
    ], []

    #subtable_columns = {
    #    'details_table': ([
    #    Column('key', TEXT),
    #    ], []),
    #}

    def prepareAcquisition(self):
        self.source_url = self.config['database_structure_file']

        # prepare downloads
        self.source = hf.downloadService.addDownload(self.source_url)

        self.details_table_db_value_list = []

    def extractData(self):
        data = {
            'database_structure_file': self.source_url,
            'status': 1,
        }

        # read the downloaded files
        json_string = open(self.source.getTmpPath()).read()
        #print json_string
        #json_content = json.loads(json_string)
        #for entry in json_content:
        #    print entry

        hf_runs_table = None
        module_instances = []
        for table in hf.database.metadata.sorted_tables:
            if table.name == 'hf_runs':
                hf_runs_table = table
            elif table.name == 'module_instances':
                module_instances = select([table.c.instance,table.c.module]).execute().fetchall() 

        return data

#    def fillSubtables(self, parent_id):
#        self.subtables['details_table'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_table_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
#        details = self.subtables['details_table'].select().where(self.subtables['details_table'].c.parent_id==self.dataset['id']).execute().fetchall()
#        data['details'] = map(dict, space_tokens_details)

        hf_runs_table = None
        module_instances = []
        for table in hf.database.metadata.sorted_tables:
            if table.name == 'hf_runs':
                hf_runs_table = table
            elif table.name == 'module_instances':
                module_instances = select([table.c.instance,table.c.module]).execute().fetchall()

        database_tables = []
        for table in hf.database.metadata.sorted_tables:
            if table.name.startswith('mod_'):
                #print table.name
                table_dictionary = {}
                table_dictionary['module_table_name'] = table.name
                module_table_columns = []
                for column in table.c:
                    module_table_columns.append(column.name)
                table_dictionary['module_table_columns'] = module_table_columns
                table_dictionary['subtables'] = []
                table_dictionary['module_name'] = table.name
                #for instance, module in module_instances:
                #    if table.c.instance.foreign_keys[0] == instance:
                #        print module
                #        table_dictionary['module_name'] = module
                #        break
                #print table.c.instance
                database_tables.append(table_dictionary)
        for table in hf.database.metadata.sorted_tables:
            if table.name.startswith('sub_'):
                subtable_dictionary = {}
                module_reference = str(table.c.parent_id.foreign_keys[0].column).split('.')[0]
                for entry in database_tables:
                    if entry['module_table_name'] == module_reference:
                        subtable_dictionary['subtable_name'] = table.name
                        subtable_table_columns = []
                        for column in table.c:
                            subtable_table_columns.append(column.name)
                        subtable_dictionary['subtable_columns'] = subtable_table_columns
                        entry['subtables'].append(subtable_dictionary)
                        break

        data['database_tables'] = json.dumps(database_tables)
        data['config_settings'] = {}
        data['config_settings']['database_structure_file'] = self.config['database_structure_file']

        return data

