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
from sqlalchemy import TEXT, INT, FLOAT, Column
from BeautifulSoup import BeautifulSoup
import json

class Panda(hf.module.ModuleBase):

    site_names_example = ("praguelcg2,LRZ-LMU,CSCS-LCG2,GoeGrid,FZK-LCG2,PSNC,"
        "HEPHY-UIBK,UNI-FREIBURG,wuppertalprod,TUDresden-ZIH,"
        "MPPMU,DESY-HH,DESY-ZN,CYFRONET-LCG2,"
        "UNI-DORTMUND,FMPhI-UNIBA,IEPSAS-Kosice")

    config_keys = {
        'schedconfig_url': ('Schedconfig link', 'local||http://pandaserver.cern.ch:25080/cache/schedconfig/schedconfig.all.json'),
        'panda_analysis_url': ('Panda analysis URL', 'local||http://panda.cern.ch:25980/server/pandamon/query?job=*&type=analysis&computingSite='),
        'panda_analysis_interval': ('Interval for analysis queue in hours', '3'),
        'panda_production_url': ('Panda production URL', 'local||http://panda.cern.ch:25980/server/pandamon/query?job=*&type=production&computingSite='),
        'panda_production_interval': ('Interval for production queue in hours', '3'),
        'site_names': ('Site names', site_names_example),
        'failed_warning': ('Failed rate that triggers warning status', '30'),
        'failed_critical': ('Failed rate that triggers critical status', '50'),
    }

    config_hint = ("Specify one or multiple queues of sites "
        "you would like to monitor. A queue <queue> of type "
        "<analysis|production> for site <site> "
        "(that has to be mentioned in parameter site_names "
        "in order to be taken into account) is specified as follows: "
        "<site>_<analysis|production> = <queue>. "
        "Several queues for the same site are given "
        "in the same parameter, separated by commas.")

    table_columns = [
        Column('site_names', TEXT),
    ], []

    subtable_columns = {
        'site_details': ([
        Column('site_name', TEXT),
        Column('queue_name', TEXT),
        Column('queue_link', TEXT),
        Column('queue_type', TEXT),
        Column('efficiency', FLOAT),
        Column('status', TEXT),
        Column('active_jobs', INT),
        Column('running_jobs', INT),
        Column('defined_jobs', INT),
        Column('holding_jobs', INT),
        Column('finished_jobs', INT),
        Column('failed_jobs', INT),
        Column('cancelled_jobs', INT),
    ], [])}

    def prepareAcquisition(self):
        self.source_url = 'www.google.com'
        # get the site names from the configuration file
        self.site_names_nolist = ''
        try:
            self.site_names_nolist = self.config['site_names']
        except hf.ConfigError, ex:
            raise hf.exceptions.ConfigError('"%s"' % str(ex))

        self.site_names = self.site_names_nolist.split(',')
        self.queue_names = {}

        for site in self.site_names:
            self.queue_names[site] = {}
            (self.queue_names[site])['analysis'] = []
            (self.queue_names[site])['production'] = []
            try:
                analysis_queues = self.config[site.lower()+'_analysis'].split(',') # for each site get the analysis queues
                (self.queue_names[site])['analysis'] = analysis_queues
                if not analysis_queues:
                    print('WARNING: Site '+site+' does not specify any analysis queues!')
                else:
                    all_empty = 1 # check whether list objects are empty strings
                    for queue in analysis_queues:
                        if queue != '':
                            all_empty = False
                    if all_empty == 1:
                        print('WARNING: Site '+site+' does not specify any analysis queues!')
            except hf.ConfigError, ex:
                pass
                #raise hf.exceptions.ConfigError('"%s"' % str(ex))
            try:
                production_queues = self.config[site.lower()+'_production'].split(',') # for each site get the production queues
                (self.queue_names[site])['production'] = production_queues
                if not production_queues:
                    print('WARNING: Site '+site+' does not specify any production queues!')
                else:
                    all_empty = 1 # check whether list objects are empty strings
                    for queue in production_queues:
                        if queue != '':
                            all_empty = False
                    if all_empty == 1:
                        print('WARNING: Site '+site+' does not specify any production queues!')
            except hf.ConfigError, ex:
                pass
                #raise hf.exceptions.ConfigError('"%s"' % str(ex))

        # get all urls from the configuration file and queue them for downloading
        schedconfig_url = ''
        try:
            schedconfig_url = self.config['schedconfig_url']
        except hf.ConfigError, ex:
            raise hf.exceptions.ConfigError('"%s"' % str(ex))

        self.schedconfig_source = hf.downloadService.addDownload(schedconfig_url)

        self.panda_production_url = ''
        try:
            self.panda_production_url = self.config['panda_production_url']
        except hf.ConfigError, ex:
            raise hf.exceptions.ConfigError('"%s"' % str(ex))

        self.panda_production_interval = ''
        try:
            self.panda_production_interval = self.config['panda_production_interval']
        except hf.ConfigError, ex:
            raise hf.exceptions.ConfigError('"%s"' % str(ex))

        self.panda_analysis_url = ''
        try:
            self.panda_analysis_url = self.config['panda_analysis_url']
        except hf.ConfigError, ex:
            raise hf.exceptions.ConfigError('"%s"' % str(ex))

        self.panda_analysis_interval = ''
        try:
            self.panda_analysis_interval = self.config['panda_analysis_interval']
        except hf.ConfigError, ex:
            raise hf.exceptions.ConfigError('"%s"' % str(ex))

        # put together the urls for all queues and queue them for downloading
        self.site_sources = {}
        for site in self.site_names:
            self.site_sources[site] = {}
            (self.site_sources[site])['analysis'] = []
            (self.site_sources[site])['production'] = []
            for analysis_queue in (self.queue_names[site])['analysis']:
                url = self.panda_analysis_url+analysis_queue+'&hours='+self.panda_analysis_interval
                #print url
                download = hf.downloadService.addDownload(url)
                (self.site_sources[site])['analysis'].append(download)
            for production_queue in (self.queue_names[site])['production']:
                url = self.panda_production_url+production_queue+'&hours='+self.panda_production_interval
                #print url
                download = hf.downloadService.addDownload(url)
                (self.site_sources[site])['production'].append(download)
        
        self.details_db_value_list = []

    def extractData(self):
        data = {
            'site_names':(self.site_names_nolist),
            'status':1
        }

        # check for download errors
        schedconfig_content = ''
        if self.schedconfig_source.errorOccured():
            print('WARNING: The url '+self.schedconfig_source.getSourceUrl()+' could not be downloaded!')
        else:
            schedconfig_content = open(self.schedconfig_source.getTmpPath()).read()

        for site in self.site_names:
            for source in (self.site_sources[site])['analysis']:
                if source.errorOccured():
                    print('WARNING: The url '+source.getSourceUrl()+' could not be downloaded!')
            for source in (self.site_sources[site])['production']:
                if source.errorOccured():
                    print('WARNING: The url '+source.getSourceUrl()+' could not be downloaded!')

        queue_details = {}
        for site in self.site_names:
            grid_site_info = cloud_class(schedconfig_content)
            # parse information from the file for each analysis queue
            for source, queue in map(None, (self.site_sources[site])['analysis'], (self.queue_names[site])['analysis']):
                queue_info = {}
                source_content = open(source.getTmpPath()).read()
                grid_site_info.panda_info_preprocessing(source_content)
                analysis_info=grid_site_info.get_queue_status(queue)
                queue_info['site_name'] = site
                queue_info['queue_name'] = queue
                queue_info['queue_link'] = self.panda_analysis_url.split('|')[2]+queue+'&hours='+self.panda_analysis_interval
                queue_info['queue_type'] = 'analysis'
                queue_info['status'] = analysis_info[1]
                queue_info['active_jobs'] = grid_site_info.get_numberof_activated_jobs()
                queue_info['running_jobs'] = grid_site_info.get_numberof_running_jobs()
                queue_info['defined_jobs'] = grid_site_info.get_numberof_defined_jobs()
                queue_info['holding_jobs'] = grid_site_info.get_numberof_holding_jobs()
                queue_info['finished_jobs'] = grid_site_info.get_numberof_finished_jobs()
                queue_info['failed_jobs'] = grid_site_info.get_numberof_failed_jobs()
                #if int(queue_info['failed_jobs']) >= int(self.config['failed_warning']) and \
                    #int(queue_info['failed_jobs']) < int(self.config['failed_critical']):
                #    data['status'] = min(data['status'],0.5)
                #elif int(queue_info['failed_jobs']) >= int(self.config['failed_critical']):
                #    data['status'] = min(data['status'],0.)
                queue_info['cancelled_jobs'] = grid_site_info.get_numberof_cancelled_jobs()
                # calculate the efficiency
                if (queue_info['finished_jobs'] + queue_info['failed_jobs']) != 0:
                    queue_info['efficiency'] = (queue_info['finished_jobs']*100)/(queue_info['finished_jobs'] + queue_info['failed_jobs'])
                else:
                    queue_info['efficiency'] = 0
                # determine the module status
                if 100. - float(queue_info['efficiency']) >= int(self.config['failed_warning']) \
                    and 100. - float(queue_info['efficiency']) < int(self.config['failed_critical']):
                    data['status'] = min(data['status'],0.5)
                elif 100. - float(queue_info['efficiency']) >= int(self.config['failed_critical']):
                    data['status'] = min(data['status'],0.)
                # add to array
                queue_details[site+'_'+queue+'_analysis'] = queue_info

            # parse information from the file for each production queue
            for source, queue in map(None, (self.site_sources[site])['production'], (self.queue_names[site])['production']):
                queue_info = {}
                source_content = open(source.getTmpPath()).read()
                grid_site_info.panda_info_preprocessing(source_content)
                production_info=grid_site_info.get_queue_status(queue)
                queue_info['site_name'] = site
                queue_info['queue_name'] = queue
                queue_info['queue_link'] = self.panda_production_url.split('|')[2]+queue+'&hours='+self.panda_production_interval
                queue_info['queue_type'] = 'production'
                queue_info['status'] = production_info[1]
                if queue_info['status'].lower() == 'online':
                    data['status'] = min(data['status'], 1)
                elif (queue_info['status'].lower()) == 'offline' or (queue_info['status'].lower()) == 'brokeroff':
                    data['status'] = min(data['status'], 0.5)
                else:
                    data['status'] = min(data['status'], 0)
                queue_info['active_jobs'] = grid_site_info.get_numberof_activated_jobs()
                queue_info['running_jobs'] = grid_site_info.get_numberof_running_jobs()
                queue_info['defined_jobs'] = grid_site_info.get_numberof_defined_jobs()
                queue_info['holding_jobs'] = grid_site_info.get_numberof_holding_jobs()
                queue_info['finished_jobs'] = grid_site_info.get_numberof_finished_jobs()
                queue_info['failed_jobs'] = grid_site_info.get_numberof_failed_jobs()
                queue_info['cancelled_jobs'] = grid_site_info.get_numberof_cancelled_jobs()
                # calculate the efficiency
                if (queue_info['finished_jobs'] + queue_info['failed_jobs']) != 0:
                    queue_info['efficiency'] = (queue_info['finished_jobs']*100)/(queue_info['finished_jobs'] + queue_info['failed_jobs'])
                else:
                    queue_info['efficiency'] = 0

                # add to array
                queue_details[site+'_'+queue+'_production'] = queue_info

        self.details_db_value_list = [{'site_name':(queue_details[queue])['site_name'], \
            'queue_type':(queue_details[queue])['queue_type'], \
            'queue_name':(queue_details[queue])['queue_name'], \
            'queue_link':(queue_details[queue])['queue_link'], \
            'efficiency':(queue_details[queue])['efficiency'], \
            'status':(queue_details[queue])['status'], \
            'active_jobs':(queue_details[queue])['active_jobs'], \
            'running_jobs':(queue_details[queue])['running_jobs'], \
            'defined_jobs':(queue_details[queue])['defined_jobs'], \
            'holding_jobs':(queue_details[queue])['holding_jobs'], \
            'finished_jobs':(queue_details[queue])['finished_jobs'], \
            'failed_jobs':(queue_details[queue])['failed_jobs'], \
            'cancelled_jobs':(queue_details[queue])['cancelled_jobs']} for queue in queue_details]

        return data

    def fillSubtables(self, parent_id):
        self.subtables['site_details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        site_details = self.subtables['site_details'].select().where(self.subtables['site_details'].c.parent_id==self.dataset['id']).execute().fetchall()

        # calculate the number of queues for each site in order to determine the rowspan for the table
        site_names = self.dataset['site_names']
        rowspan = {}
        for site in site_names.split(','):
            rowspan[site] = 0
            for detail in site_details:
                if detail['site_name'] == site:
                    rowspan[site] += 1

        data['site_details'] = map(dict, site_details)
        data['rowspan'] = rowspan

        data['config'] = {}
        data['config']['failed_warning'] = int(self.config['failed_warning'])
        data['config']['failed_critical'] = int(self.config['failed_critical'])

        return data


class cloud_class(object):

    def __init__(self,schedconfig_content):
        content = schedconfig_content
        self.json_source=json.loads(content,'utf-8')
        self.resource=[]

    def panda_info_preprocessing(self,html_content):
        src=html_content
        l=True
        while l:
            n=len(src)
            if ("Click job number to see details." in src[0:n/2] or \
                "Listing limited to search depth of" in src[0:n/2]) and \
                ("defined" in src[0:n/2] or "activated" in src[0:n/2] or \
                "running" in src[0:n/2] or "holding" in src[0:n/2] or \
                "transferring" in src[0:n/2] or "finished" in src[0:n/2] or \
                "cancelled" in src[0:n/2]) and "Users" in src[0:n/2] and "Releases" in src[0:n/2]:
                l=True
                src=src[0:(n/2)+15]
            elif ("Click job number to see details." in src[n/2:n] or \
                "Listing limited to search depth of" in src[0:n/2]) and \
                ("defined" in src[n/2:n] or "activated" in src[n/2:n] or \
                "running" in src[n/2:n] or  "holding" in src[n/2:n] or \
                "transferring" in src[n/2:n] or "finished" in src[n/2:n] or \
                "cancelled" in src[n/2:n]) and "Users" in src[n/2:n] and "Releases" in src[n/2:n]:
                l=True
                src=src[(n/2)-15:n]
            else:
                l=False
        i=0
        while i<len(src)-len("Click job number to see details."):
            if src[i:i+len("Click job number to see details.")]=="Click job number to see details." or \
                src[i:i+len("Listing limited to search depth of")]=="Listing limited to search depth of":
                src=src[i:len(src)]
                break
            i+=1
        soup = BeautifulSoup(src)
        src_text = soup.findAll(text=True)
        src_text = ''.join(src_text)
        src_text=src_text.split(" ")
        self.resource=src_text

    def get_queue_status(self,queue):
        queue_status="unknown"
        analysis_queues=[]
        production_queues=[]
        if "analy" in queue.lower():
            for item in self.json_source:
                if queue == self.json_source[item]["siteid"]:
                    if str(self.json_source[item]["gatekeeper"])!="to.be.set":
                        analysis_queues.append("%s %s"%(str(self.json_source[item]["gatekeeper"]),str(self.json_source[item]["status"])))
                    else:
                        analysis_queues.append("%s %s"%(str(self.json_source[item]["siteid"]),str(self.json_source[item]["status"])))
                    if self.json_source[item]["status"]=="online":
                        queue_status="online"
                    elif queue_status!="online" and self.json_source[item]["status"]!="online":
                        queue_status=self.json_source[item]["status"]
                else:
                    continue
            return analysis_queues,queue_status
        else:
            for item in self.json_source:
                if queue == self.json_source[item]["siteid"]:
                    production_queues.append("%s %s"%(str(self.json_source[item]["gatekeeper"]),str(self.json_source[item]["status"])))
                    if self.json_source[item]["status"]=="online":
                        queue_status="online"
                    elif queue_status!="online" and self.json_source[item]["status"]!="online":
                        queue_status=self.json_source[item]["status"]
                else:
                    continue
            return production_queues,queue_status
        return queue_status

    #def get_queue_gatekeeper_info(self,queue):
    #    return 0

    def get_numberof_activated_jobs(self):
        activated_jobs=0
        for item in self.resource:
            if "activated" in item:
                try:
                    activated_jobs=int(item.split(":")[1])
                    return activated_jobs
                except ValueError:
                    activated_jobs=0
        return activated_jobs

    def get_numberof_running_jobs(self):
        running_jobs=0
        for item in self.resource:
            if "running" in item:
                try:
                    running_jobs=int(item.split(":")[1])
                    return running_jobs
                except ValueError:
                    running_jobs=0
        return running_jobs                

    def get_numberof_defined_jobs(self):
        defined_jobs=0
        for item in self.resource:
            if "defined" in item:
                try:
                    defined_jobs=int(item.split(":")[1])
                    return defined_jobs
                except ValueError:
                    defined_jobs=0
        return defined_jobs

    def get_numberof_holding_jobs(self):
        holding_jobs=0
        for item in self.resource:
            if "holding" in item:
                try:
                    holding_jobs=int(item.split(":")[1])
                    return holding_jobs
                except ValueError:
                    holding_jobs=0
        return holding_jobs

    def get_numberof_finished_jobs(self):
        finished_jobs=0
        for item in self.resource:
            if "finished" in item:
                try:
                    finished_jobs=int(item.split(":")[1])
                    return finished_jobs
                except ValueError:
                    finished_jobs=0
        return finished_jobs

    def get_numberof_failed_jobs(self):
        failed_jobs=0
        for item in self.resource:
            if "failed" in item:
                try:
                    failed_jobs=int(item.split(":")[1])
                    return failed_jobs
                except ValueError:
                    failed_jobs=0
        return failed_jobs

    def get_numberof_cancelled_jobs(self):
        cancelled_jobs=0
        for item in self.resource:
            if "cancelled" in item:
                try:
                    cancelled_jobs=int(item.split(":")[1])
                    return cancelled_jobs
                except ValueError:
                    cancelled_jobs=0
        return cancelled_jobs

