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

import hf
import time
import os
from sqlalchemy import *

class HappyHealth(hf.module.ModuleBase):
    config_keys = {
        'directory':('Path of the directory to show its space', ''),
        'space_stat_warn':('Percentage of free space until status is OK', '10'),
        'space_stat_crit':('Percentage of free space when status is critical', '5'),
        'load0_stat_warn':('load until status is OK', '2'),
        'load0_stat_crit':('load when status is critical', '5'),
        'load1_stat_warn':('load until status is OK', '2'),
        'load1_stat_crit':('load when status is critical', '5'),
        'load2_stat_warn':('load until status is OK', '2'),
        'load2_stat_crit':('load when status is critical', '5'),
    }
    config_hint = ''

    table_columns = [
        Column("space_total", FLOAT),
        Column("space_used", FLOAT),
        Column("space_free", FLOAT),
        Column("avg_load_last_1min", FLOAT),
        Column("avg_load_last_5min", FLOAT),
        Column("avg_load_last_15min", FLOAT),
        Column("status", FLOAT)
    ], []

    def getTemplateData(self):
        data =  hf.module.ModuleBase.getTemplateData(self)
        for option in self.config_keys:
            data["dataset"][option] = self.config[option]
        return data

    def prepareAcquisition(self):
        self.source_url = 'local'
        self.path = self.config["directory"]
        self.sp_limit1 = float(self.config["space_stat_warn"])
        self.sp_limit2 = float(self.config["space_stat_crit"])
        self.load_limit_warn = [self.config["load0_stat_warn"]]
        self.load_limit_crit = [self.config["load0_stat_crit"]]
        self.load_limit_warn.append(self.config["load1_stat_warn"])
        self.load_limit_crit.append(self.config["load1_stat_crit"])
        self.load_limit_warn.append(self.config["load2_stat_warn"])
        self.load_limit_crit.append(self.config["load2_stat_crit"])
        self.load_limit_warn = map(float, self.load_limit_warn)
        self.load_limit_crit = map(float, self.load_limit_crit)

    def extractData(self):
        st = os.statvfs(self.path)
        load_status = [0, 1, 2]
        totalspace = st.f_blocks*st.f_frsize
        usedspace = (st.f_blocks-st.f_bavail)*st.f_frsize
        freespace = st.f_bavail*st.f_frsize
        freespace_percentage = 100.0*float(freespace)/float(totalspace)
        load = os.getloadavg()
        data = {
        }
        data.update({
            "space_total":totalspace,
            "space_used":usedspace,
            "space_free":freespace,
            "avg_load_last_1min":load[0],
            "avg_load_last_5min":load[1],
            "avg_load_last_15min":load[2]
        })
        if freespace_percentage < self.sp_limit2:
            sp_status = 0.0
        elif freespace_percentage < self.sp_limit1:
            sp_status = 0.5
        else:
            sp_status = 1.0

        for i in range(3):
            if load[i] > self.load_limit_crit[i]:
                load_status[i] = 0.0
            elif load[i] > self.load_limit_warn[i]:
                load_status[i] = 0.5
            else:
                load_status[i] = 1.0
        data["status"] = min(load_status[0],load_status[1],load_status[2],sp_status)

        return data
