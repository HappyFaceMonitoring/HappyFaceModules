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
from HTMLParser import HTMLParser
from datetime import datetime
from BeautifulSoup import BeautifulSoup

class Apel(hf.module.ModuleBase):
    config_keys = {
        'source_html': ('HTML Source File', 'local||http://goc-accounting.grid-support.ac.uk/rss/YourSite_Sync.html'),
        'source_xml': ('XML Source File', 'local||http://goc-accounting.grid-support.ac.uk/rss/YourSite_ApelSync.xml'),
        'daysagowarning': ('Days without sync that lead to warning status.', 3),
        'daysagocritical': ('Days without sync that lead to critical status', 5),
        'displayrows': ('Number of entries to be saved and displayed', 4)
    }

    #config_hint = "Do whatever you like."

    table_columns = [
        Column('source_html', TEXT),
        Column('source_xml', TEXT),
        Column('apel_title', TEXT),
        Column('apel_description', TEXT),
        Column('apel_link', TEXT),
        Column('last_build', DATETIME),
    ], []

    subtable_columns = {
        'details_table': ([
        Column('record_start', DATETIME),
        Column('record_end', DATETIME),
        Column('record_count_database', INT), # record count in record_count_database
        Column('record_count_published', INT), # record count record_count_published
        Column('sync_status', TEXT), # synchronization status
    ], [])}


    def prepareAcquisition(self):
        # get urls from config and queue them for downloading
        url_html = self.config['source_html']
        url_xml = self.config['source_xml']
        self.source_html = hf.downloadService.addDownload(self.config['source_html'])
        self.source_xml = hf.downloadService.addDownload(self.config['source_xml'])

        self.details_table_db_value_list = []

    def extractData(self):
        data = {
            'source_html': self.config['source_html'],
            'source_xml': self.config['source_xml'],
            'status': 1
            }
        daysagowarning = int(self.config['daysagowarning'])
        daysagocritical = int(self.config['daysagocritical'])
        displayrows = self.config['displayrows']
        displayrows = int(displayrows)

        self.apel_details = {}

        # get summary status from xml file
        data_html = open(self.source_html.getTmpPath()).read().replace('\n',' ').replace('\r',' ')
        data_xml = open(self.source_xml.getTmpPath()).read()

        if len(data_html) < 2:
            data['status'] = -1

        # xml data is optional
        if len(data_xml) < 2:
            pass
        else:
            xmlroot = etree.fromstring(data_xml)

            # get title, link, and description from xml
            title = ''
            link = ''
            description = ''
            for tags in xmlroot[0]:
                if tags.tag == 'item':
                    title = tags[0].text
                    link = tags[1].text
                    description = tags[2].text
                    break

            data['apel_title'] = title
            data['apel_link'] = link
            data['apel_description'] = description.strip().lower()
 
            # set status
            if len(description) < 2 or (description.strip().lower())[0:2] != 'ok':
                data['status'] = 0

        # fetch the table from the html input
        table = TableParser(data_html, '')
        table = table.table

        # check whether there is a table
        if len(table) < 2 or len(table[1]) < 4:
            print('Error while parsing Apel HTML page: Could not find expected table.')
            data['status'] = -1

        else:
            for i in range(1, min(displayrows+1,len(table))):
                apel_detail = {}

                # fetch last table entry. it has to be 'OK' or
                # 'OK [ last published ### days ago: ####-##-## ]'
                last = table[i][-1]
                if (len(last) < 2) or ((last.strip().lower())[0:2] != 'ok'):
                    if i == 1:
                        data['status'] = min(data['status'], 0.5)
                m = re.search('published [0-9]+ days ago', last)
                if (m is not None) and (i == 1):
                    daysago = int(re.findall('[0-9]+', last)[0])
                    if daysago >= daysagocritical:
                        data['status'] = min(data['status'], 0)
                    elif daysago >= daysagowarning:
                        data['status'] = min(data['status'], 0.5)
                    else:
                        pass

                # parse information
                apel_detail['record_start'] = datetime.strptime(table[i][0], "%Y-%m-%d")
                apel_detail['record_end'] = datetime.strptime(table[i][1], "%Y-%m-%d")
                apel_detail['record_count_database'] = table[i][2]
                apel_detail['record_count_published'] = table[i][3]
                apel_detail['sync_status'] = table[i][4]

                self.apel_details[(apel_detail['record_start'].strftime("%Y-%m-%d")+'-'+apel_detail['record_end'].strftime("%Y-%m-%d"))] = apel_detail

        # get date of last build
        soup = BeautifulSoup(data_html)
        for li in soup.findAll('li'):
            if li.string:
                if li.string[0:12] == 'lastBuild : ':
                    last_build_string = li.string.split('lastBuild : ')[1][:-3]
                    last_build = datetime.strptime(last_build_string, "%Y-%m-%d %H:%M:%S")
                    data['last_build'] = last_build

        self.details_table_db_value_list = [{'record_start':(self.apel_details[apel_detail])['record_start'], 'record_end':(self.apel_details[apel_detail])['record_end'], 'record_count_database':(self.apel_details[apel_detail])['record_count_database'], 'record_count_published':(self.apel_details[apel_detail])['record_count_published'], 'sync_status':(self.apel_details[apel_detail])['sync_status']} for apel_detail in self.apel_details]

        return data

    def fillSubtables(self, parent_id):
        self.subtables['details_table'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_table_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        details = self.subtables['details_table'].select().where(self.subtables['details_table'].c.parent_id==self.dataset['id']).order_by(desc('record_start')).execute().fetchall()
        data['details'] = map(dict, details)

        data['source_html_link'] = self.config['source_html'].split('|')[2]

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
                #print('\n\n\n\nHERE WE GO!!!!!\n\n\n\n')
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

        else:
            pass


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

