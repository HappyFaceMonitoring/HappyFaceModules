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

class Uschi(hf.module.ModuleBase):
    config_keys = {
        'testname_string': ('Name of the Test', ''),
        'uschi_xml': ('Input file', '')
    }
    config_hint = ''
    
    
    table_columns = [
        Column('uschi_timestamp', TEXT),
        Column('uschi_timestamp_module', TEXT),
        Column('frequency', INT),
        Column('frequency_module', INT),
        Column('result', INT),
        Column('log', TEXT),
        Column('about', TEXT),
    ], []

    def prepareAcquisition(self):

        try:
            self.testname_string = self.config["testname_string"]
        except KeyError, e:
            raise hf.exceptions.ConfigError('Required parameter "%s" not specified' % str(e))

        if 'uschi_xml' not in self.config: raise hf.exceptions.ConfigError('uschi_xml option not set')
        self.uschi_xml = hf.downloadService.addDownload(self.config['uschi_xml'])
                
    def extractData(self):
        data = {'source_url': self.uschi_xml.getSourceUrl(),
                'uschi_timestamp': '',
                'uschi_timestamp_module': '',
                'frequency': -1,
                'frequency_module': -1,
                'status': -1,
                'result': -1,
                'log': '',
                'about': ''}

        source_tree = etree.parse(open(self.uschi_xml.getTmpPath()))
        root = source_tree.getroot()

        self.uschi_timestamp = root.get('time')

        # get the execution frequency of USCHI (in minutes)
        self.frequency = int(root.get('frequency'))

        self.log = ""
        self.about = ""

        self.test_found = False

        # get the test specific information (result, log, about)
        for sec in root.findall("tests/test"):
            if sec.get('name') == self.testname_string:

                self.test_found = True

                # test logic
                self.result = int(sec.get('result'))
                if (self.result == 0): self.status = 1.0 # happy
                elif (self.result == 1): self.status = 0.5 # neutral
                elif (self.result == 2): self.status = 0.0 # unhappy

                # get the last test execution time of the module
                self.uschi_timestamp_module = sec.get('time')

                # get frequency of the module (in minutes)
                self.frequency_module = int(sec.get('frequency'))
                
                # get the "log" and "about" information of the test
                log_data = sec.findall('log')
                about_data = sec.findall('about')
                for info in log_data:
                    for line in info.text.splitlines():
                        self.log += '<p>' + line + '</p>' # add <p> tags for HTML output
                for info in about_data:
                    for line in info.text.splitlines():
                        self.about += line
        
        if not self.test_found:
            data['error_string'] = 'Test was not found in data source.'
            data['status'] = -1
            return data

        # definition fo the database table values
        data['uschi_timestamp'] = self.uschi_timestamp
        data['uschi_timestamp_module'] = self.uschi_timestamp_module
        data['frequency'] = self.frequency
        data['frequency_module'] = self.frequency_module
        data['result'] = self.result
        data['log'] = self.log
        data['about'] = self.about
        data['status'] = self.status

        return data


