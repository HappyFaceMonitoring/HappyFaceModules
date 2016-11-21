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
from sqlalchemy import TEXT, INT, Column
from operator import itemgetter


class CondorUserprio(hf.module.ModuleBase):
    ''' define condig values to be used later'''
    config_keys = {'sourceurl': ('Source Url', '')
                   }

    table_columns = [
        Column('users', INT)
    ], []

    subtable_columns = {
        'user': ([
            Column("user", TEXT),
            Column("prio", INT)], [])
    }

    def prepareAcquisition(self):
        link = self.config['sourceurl']
        self.user_db_value_list = []
        self.source = hf.downloadService.addDownload(link)
        self.source_url = self.source.getSourceUrl()

    def extractData(self):
        #  define default values
        data = {}
        prio = []
        path = self.source.getTmpPath()

        # open file
        with open(path, 'r') as f:
            content = f.readlines()
        # if no data in condorprio, stop script and display error_msg inst. TODO
        for line in content:
            split = line.split()
            if split != [] and split[0] != "undefined" and float(split[1]) != 500.0 and split[0] != "<none>":
                user = {
                    'user': split[0],
                    'prio': round(float(split[1]), 0)
                }
                prio.append(user)
        self.user_db_value_list = sorted(prio, key=itemgetter('prio'), reverse=True)
        data["users"] = len(prio)
        return data

    # Putting Data in the Subtable to display
    def fillSubtables(self, parent_id):
        self.subtables['user'].insert().execute(
            [dict(parent_id=parent_id, **row) for row in self.user_db_value_list])

    # Making Subtable Data available to the html-output
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['user'].select().where(
            self.subtables['user'].c.parent_id == self.dataset['id']
        ).execute().fetchall()
        data["user"] = map(dict, details_list)

        return data
