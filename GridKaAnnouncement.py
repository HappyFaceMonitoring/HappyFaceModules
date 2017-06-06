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

import hf
import lxml.html
import StringIO
from sqlalchemy import TEXT, INT, Column

class GridKaAnnouncement(hf.module.ModuleBase):
    
    config_keys = {
        'crit_incidents_high': (
                'Number of high level incidents that trigger status = critical',''),
        'crit_incidents_medium': (
                'Number of medium level incidents that trigger status = critical',''),
        'warn_incidents_low': (
                'Number of low level incidents that trigger status = w8arning',''),
        'warn_incidents_info': (
                'Number of info level incidents that trigger status = warning',''),
        'crit_interventions_outage': (
                'Number of interventions with level = outage that trigger \
                status = critical',''),
        'warn_interventions_info': (
                'Number of interventions with level = at risk/info that \
                trigger status = warning',''),
        'source_url': (
                'GridKa Incidents and Interventions Report URL',''),
    }
    config_hint = ''
    
    table_columns = ([
        Column('nIncidentsHigh', INT),
        Column('nIncidentsMedium', INT),
        Column('nIncidentsLow', INT),
        Column('nIncidentsInfo', INT),
        Column('nInterventionsOutage', INT),
        Column('nInterventionsAtRisk', INT)], [])
    
    subtable_columns = {
        'incidents': ([
            Column('row_type', TEXT),
            Column('severity', TEXT),
            Column('submit_time', TEXT),
            Column('update_time', TEXT),
            Column('description', TEXT),
            Column('affecting_explanation', TEXT)], []),
        'interventions': ([
            Column('row_type', TEXT),
            Column('severity', TEXT),
            Column('submit_time', TEXT),
            Column('update_time', TEXT),
            Column('intervention_start', TEXT),
            Column('intervention_end', TEXT),
            Column('description', TEXT),
            Column('affecting_explanation', TEXT)], [])}
    
    def prepareAcquisition(self):
        try:
            self.nCritIncidentsHigh = int(self.config['crit_incidents_high'])
            self.nCritIncidentsMedium = int(self.config['crit_incidents_medium'])
            self.nWarnIncidentsLow = int(self.config['warn_incidents_low'])
            self.nWarnIncidentsInfo = int(self.config['warn_incidents_info'])
            self.nCritInterventionsOutage = int(self.config['crit_interventions_outage'])
            self.nWarnInterventionsInfo = int(self.config['warn_interventions_info'])
        except KeyError, ex:
            raise hf.exceptions.ConfigError(
                    'Required parameter "%s" not specified' % str(ex))
        if 'source_url' not in self.config:
            raise hf.exceptions.ConfigError('No source URL specified')
        self.source = hf.downloadService.addDownload(self.config['source_url'])
        self.source_url = self.source.getSourceUrl()
        self.incidents_db_value_list = []
        self.interventions_db_value_list = []
    
    def extractData(self):
        data = {'source_url': self.source.getSourceUrl()}
        webpage = open(self.source.getTmpPath())
        strwebpage = webpage.read().replace("<br>","\n").replace("<br/>","\n")
        tree = lxml.html.parse(StringIO.StringIO(strwebpage))
        itables = 0
        nIncidentsHigh = 0
        nIncidentsMedium = 0
        nIncidentsLow = 0
        nIncidentsInfo = 0
        nInterventionsOutage = 0
        nInterventionsAtRisk = 0
        for tables in tree.iter('table'):
            if itables == 0:
                for irows in range(2,len(tables)):
                    row = tables[irows]
                    severitylevel = row[0][0][0].get('src').replace(
                            '/monitoring/status/images/','').replace(
                            '_dot.gif','')
                    if severitylevel == 'red':
                        incident_type = 'critical'
                        severitylevel = 'high'
                        nIncidentsHigh += 1
                    elif severitylevel == 'orange':
                        incident_type = 'warning'
                        severitylevel = 'medium'
                        nIncidentsMedium += 1
                    elif severitylevel == 'yellow':
                        incident_type = 'warning'
                        severitylevel = 'low'
                        nIncidentsLow += 1
                    elif severitylevel == 'green':
                        incident_type = 'ok'
                        severitylevel = 'info'
                        nIncidentsInfo += 1
                    Incident = {}
                    Incident['row_type'] = incident_type
                    Incident['severity'] = severitylevel

                    time = row[1].text_content()
                    if '\n' in time:
                        submit_time = time.split('\n')[0]
                        update_time = time.split('\n')[1]
                        Incident['submit_time'] = submit_time if \
                                (submit_time!='' and not submit_time is None) else 'n/a'
                        Incident['update_time'] = update_time if \
                                (update_time!='' and not submit_time is None) else 'n/a'
                    elif time <> '':
                        Incident['submit_time'] = time
                        Incident['update_time'] = 'n/a'
                    else:
                        Incident['submit_time'] = 'n/a'
                        Incident['update_time'] = 'n/a'

                    if row[2].text == None:
                        Incident['description'] = 'n/a'
                    else:
                        Incident['description'] = \
                                row[2].text_content().replace("\n","<br/>")
                    if row[3].text == None:
                        Incident['affecting_explanation'] = 'n/a'
                    else:
                        Incident['affecting_explanation'] = \
                                row[3].text_content().replace("\n","<br/>")
                    self.incidents_db_value_list.append(Incident)
                data['nIncidentsHigh'] = nIncidentsHigh
                data['nIncidentsMedium'] = nIncidentsMedium
                data['nIncidentsLow'] = nIncidentsLow
                data['nIncidentsInfo'] = nIncidentsInfo
            elif itables == 1:
                for irows in range(2,len(tables)):
                    row = tables[irows]
                    severitylevel = row[0][0][0].get('src').replace(
                           '/monitoring/status/images/','').replace('_dot.gif','')
                    if severitylevel == 'red':
                        intervention_type = 'critical'
                        severitylevel = 'outage'
                        nInterventionsOutage += 1
                    elif severitylevel == 'green':
                        intervention_type = 'ok'
                        severitylevel = 'info/at risk'
                        nInterventionsAtRisk += 1
                    Intervention = {}
                    Intervention['row_type'] = intervention_type
                    Intervention['severity'] = severitylevel

                    SubmitUpdateTime = row[1].text_content()
                    if '\n' in SubmitUpdateTime:
                        submit_time = SubmitUpdateTime.split('\n')[0]
                        update_time = SubmitUpdateTime.split('\n')[1]
                        Intervention['submit_time'] = submit_time if (
                                submit_time!='' and not submit_time is None
                                ) else 'n/a'
                        Intervention['update_time'] = update_time if (
                                update_time!='' and not submit_time is None
                                ) else 'n/a'
                    elif SubmitUpdateTime <> '':
                        Intervention['submit_time'] = SubmitUpdateTime
                        Intervention['update_time'] = 'n/a'
                    else:
                        Intervention['submit_time'] = 'n/a'
                        Intervention['update_time'] = 'n/a'

                    StartEnd = row[2].text_content()
                    if '\n' in StartEnd:
                        intervention_start = StartEnd.split('\n')[0]
                        intervention_end = StartEnd.split('\n')[1]
                        Intervention['intervention_start'] = intervention_start if (
                                intervention_start!='' and not intervention_start is None
                                ) else 'n/a'
                        Intervention['intervention_end'] = intervention_end if (
                                intervention_end!='' and not intervention_end is None
                                ) else 'n/a'
                    elif StartEnd <> '':
                        Intervention['intervention_start'] = StartEnd
                        Intervention['intervention_end'] = 'n/a'
                    else:
                        Intervention['intervention_start'] = 'n/a'
                        Intervention['intervention_end'] = 'n/a'

                    if row[3].text == None:
                        Intervention['description'] = 'n/a'
                    else:
                        Intervention['description'] = \
                                row[3].text_content().replace("\n","<br/>")
                    if row[4].text == None:
                        Intervention['affecting_explanation'] = 'n/a'
                    else:
                        Intervention['affecting_explanation'] = \
                                row[4].text_content().replace("\n","<br/>")
                    self.interventions_db_value_list.append(Intervention)
                data['nInterventionsOutage'] = nInterventionsOutage
                data['nInterventionsAtRisk'] = nInterventionsAtRisk
            itables += 1
            
        # check if numbers of different incidents and interventions
        # exceed the limits and set status accordingly
        if nIncidentsHigh >= self.nCritIncidentsHigh or \
                nIncidentsMedium >= self.nCritIncidentsMedium or \
                nInterventionsOutage >= self.nCritInterventionsOutage:
            data['status'] = 0.0
        elif nIncidentsLow >= self.nWarnIncidentsLow or \
                nIncidentsInfo >= self.nWarnIncidentsInfo or \
                nInterventionsAtRisk >= self.nWarnIncidentsInfo:
            data['status'] = 0.5
        else:
            data['status'] = 1.0

        return data

    def fillSubtables(self, parent_id):
        self.subtables['incidents'].insert().execute(
                [dict(parent_id=parent_id, **row) for row in self.incidents_db_value_list])
        self.subtables['interventions'].insert().execute(
                [dict(parent_id=parent_id, **row) for row in self.interventions_db_value_list])
    
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        incident_list = self.subtables['incidents'].select().where(
                self.subtables['incidents'].c.parent_id==self.dataset['id']
                ).execute().fetchall()
        data['incident_list'] = map(dict, incident_list)
        intervention_list = self.subtables['interventions'].select().where(
                self.subtables['interventions'].c.parent_id==self.dataset['id']
                ).execute().fetchall()
        data['intervention_list'] = map(dict, intervention_list)
        return data
