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
import re
from datetime import datetime
from HTMLParser import HTMLParser

class Nagios(hf.module.ModuleBase):
    config_keys = {
        'nagios_url': ('Nagios status webpage', 'local|--user=username --password=password --force-html|http://path/to/nagios/'),
    }

    config_hint = 'Username and password for accessing nagios have to be put in the config file. Create a view-only user for that purpose.'

    table_columns = [
        Column('nagios_url', TEXT),
        Column('services_ok', INT), # number of services that are ok
        Column('services_not_ok', INT), # number of services that are not ok
        Column('services_critical', INT) # number of services with a critical status
    ], []

    subtable_columns = {
        'services': ([
        Column('host_name', TEXT),
        Column('host_link', TEXT),
        Column('service_name', TEXT),
        Column('service_link', TEXT),
        Column('status_short', TEXT),
        Column('status_long', TEXT),
        Column('status_machine_readable', TEXT),
        Column('lastcheck', DATETIME), # datetime of last check
        Column('lastcheck_duration', TEXT), # lastcheck_duration of last check
        Column('attempt', TEXT) # specifies the number of attempts
        ], [])}

    def prepareAcquisition(self):
        # get nagios root url and append path to status.cgi that will be parsed
        nagios_url = self.config['nagios_url']
        if nagios_url[-1] != '/':
            nagios_url += '/'
        self.source = hf.downloadService.addDownload(nagios_url+'cgi-bin/status.cgi')

        self.services_db_value_list = []

    def extractData(self):
        data = {
            'nagios_url': self.config['nagios_url'],
            'services_ok': 0,
            'services_not_ok': 0,
            'services_critical': 0
        }

        # get the nagios url only and append the path to cgi-bin
        nagios_url_bin = data['nagios_url'].split('|')[2] # url of the nagios server
        if nagios_url_bin[-1] != '/':
            nagios_url_bin += '/'
        nagios_url_bin += 'cgi-bin/'

        #data['source_url'] = self.source.getSourceUrl()

        # open resource and prepare for parsing
        self.resource = open(self.source.getTmpPath()).read().replace('\n',' ').replace('\r',' ')

        # all content of the table with class 'status' is parsed into arrays
        tableParser = TableParser(self.resource, 'status')

        # fetch the arrays' content
        self.services = {}
        curhost_name = ''
        curhost_link = ''

        for entry in tableParser.table:
            # too short entries are not valid
            if len(entry) < 7:
                continue

            # build a new service from the entry's info
            service = {}

            # read info about host from first column
            # if first column is empty, keep old host's info
            if len(entry[0]) > 1:
                host = ContentParser(entry[0][0])
                if host.content.strip() != '':
                    curhost_name = host.content.strip()
                    curhost_link = host.url[0]

            service['host_name'] = curhost_name
            service['host_link'] = nagios_url_bin+curhost_link

            # read info about service from second column
            ser = ContentParser(entry[1][0])

            service['service_name'] = ser.content.strip()
            service['service_link'] = nagios_url_bin+ser.url[0]

            # read info about service status from third and seventh column
            service['status_short'] = entry[2]
            service['status_long'] = entry[6]

            # read more info about state
            time_lastcheck = entry[3]
            service['lastcheck'] = datetime.utcfromtimestamp(float((datetime.strptime(time_lastcheck, '%m-%d-%Y %H:%M:%S')).strftime('%s'))) # convert from local time to UTC
            service['lastcheck_duration'] = entry[4]
            service['attempt'] = entry[5]

            # generate a machine readable column, therefore take first number of status_long
            number = 0
            if re.search('[0-9.]*[0-9]+', service['status_long']):
                number = re.search('[0-9.]*[0-9]+', service['status_long']).group(0)
            else:
                number = -1
            service['status_machine_readable'] = number

            # add to array
            self.services[(service['host_name']+'-'+service['service_name'])] = service

        # determine the status of the module
        data['status'] = 1.0

        for s in self.services:
            service = self.services[s]

            if service['status_short'].lower() == 'critical':
                data['services_critical'] += 1
                # critical services give critical status, except for time outs
                if service['status_long'].lower().find('service check timed out') >= 0:
                    data['status'] = min(data['status'], 0.5)
                else:
                    data['status'] = min(data['status'], 0)

            elif service['status_short'].lower() != 'ok':
                data['services_not_ok'] += 1
                data['status'] = min(data['status'], 0.5)

            if service['status_short'].lower() == 'ok':
                data['services_ok'] += 1

        self.services_db_value_list = [{'host_name':(self.services[service])['host_name'], 'host_link':(self.services[service])['host_link'], 'service_name':(self.services[service])['service_name'], 'service_link':(self.services[service])['service_link'], 'status_short':(self.services[service])['status_short'], 'status_long':(self.services[service])['status_long'], 'status_machine_readable':(self.services[service])['status_machine_readable'], 'lastcheck':(self.services[service])['lastcheck'], 'lastcheck_duration':(self.services[service])['lastcheck_duration'], 'attempt':(self.services[service])['attempt']} for service in self.services]
        
        return data

    def fillSubtables(self, parent_id):
        self.subtables['services'].insert().execute([dict(parent_id=parent_id, **row) for row in self.services_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        service_info = self.subtables['services'].select().where(self.subtables['services'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['service_info'] = map(dict, service_info)
        return data


class TableParser(HTMLParser):

    def __init__(self, s, tclass):
        # parse given string s, seaching for first table with class tclass
        # all rows and within these all columns will be saved into the array
        # table. This will be done recursively with all subtables.
        # if a table only contains one row, indexing for rows will be skipped
        # if a row only contains one column, indexing for columns will be skipped
        # if no table is found, the full input is returned

        # if no tclass is given, the first table found will be used

        HTMLParser.__init__(self)
        self.tstart = 0
        self.tend = 0
        self.foundTable = False
        self.TableDepth = 0

        self.tclass = tclass
        self.tstring = s
        self.table = []
        self.trow = []
        self.tdstart = 0
        self.tdend = 0

        self.feed(s)

        # check if given html string is complete
        self.incomplete = False
        try:
            self.close()
        except:
            self.incomplete = True
        
        if len(self.table) == 0:
            if self.foundTable:
                # empty table found
                self.table = ''
            else:
                # if no table found, then return full input string
                self.table = self.tstring

        elif len(self.table) == 1:
            # if only one row, then skip indexing rows
            self.table = self.table[0]


    def handle_starttag(self, tag, attributes):

        if tag == 'table':

            if self.foundTable:
                if self.TableDepth >= 0:
                    self.TableDepth += 1
                else:
                    self.TableDepth = -1

            else:
                # we search for table of class tclass
                if self.tclass != '':
                    for name, value in attributes:
                        if name == 'class' and value == self.tclass:
                            self.foundTable = True
                            self.tstart = self.getpos()[1] + len(self.get_starttag_text())
                else:
                    self.foundTable = True

        elif tag == 'tr':
            if self.foundTable == True and self.TableDepth == 0:
                self.trow = []

        elif tag == 'td':
            if self.foundTable == True and self.TableDepth == 0:
                self.tdstart = self.getpos()[1] + len(self.get_starttag_text())


    def handle_endtag(self, tag):

        if tag == 'table':

            if self.foundTable:
                if self.TableDepth >= 0:
                    self.TableDepth -= 1

                    if self.TableDepth == -1:
                        self.tend = self.getpos()[1]
                        self.tstring[self.tstart:self.tend]

                else:
                    self.TableDepth = -1


        elif tag == 'tr':
            if self.foundTable == True and self.TableDepth == 0:
                # it tr did not contain any td, it will not be added
                if len(self.trow) == 1:
                    # if tr does only contain one td, skip indexing tds
                    self.table.append(self.trow[0])
                elif len(self.trow) > 1:
                    self.table.append(self.trow)

        elif tag == 'td':
            if self.foundTable == True and self.TableDepth == 0:
                self.tdend = self.getpos()[1]
                td = self.tstring[self.tdstart:self.tdend]
                td = TableParser(td, '').table
                self.trow.append(td)



class ContentParser(HTMLParser):

    def __init__(self, s):
        # parse given string s, adding all content into self.content, adding all
        # hrefs to self.url and adding all img.srcs to self.img and all alts to
        # self.alt

        HTMLParser.__init__(self)
        self.cstring = s

        self.content = ''
        self.url = []
        self.img = []
        self.alt = []

        self.feed(s)
        self.close()

    def handle_starttag(self, tag, attributes):

        for name, value in attributes:
            if name == 'href':
                self.url.append(value)
            elif tag == 'img' and name == 'src':
                self.img.append(value)
            elif tag == 'alt':
                self.alt.append(value)

    def handle_data(self, data):
        self.content += data

