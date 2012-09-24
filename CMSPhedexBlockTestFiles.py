# -*- coding: utf-8 -*-
import hf, lxml, logging
from datetime import datetime
from sqlalchemy import *
from lxml import etree

class CMSPhedexBlockTestFiles(hf.module.ModuleBase):
    
    config_keys = {
        'input_xml_age_limit': ('Module turns yellow when input xml file is older than input_xml_age_limit days', '3'),
        'blocktest_xml': ('URL of the input blocktest xml file', ''),
        'filter' : ('A list of keywords. Every file which contains a (sub)string from this list is rejected', ''),
        'forced_pass' : ('A list of keywords. Every file which contains a (sub)string from this list is forced to pass the filter', '')
    }
    config_hint = ''
    
    table_columns = [
        Column("failed_blocks_raw", INT),
        Column("failed_blocks", INT),
        Column("failed_total_files_raw", INT),
        Column("failed_total_files", INT),
        Column("request_date", TEXT),
        Column('request_timestamp', INT),
        Column("data_id", TEXT),
    ], []

    subtable_columns = {
        
        "details": ([
            Column('block', TEXT),
            Column('isfile', INT),
            Column('fails', INT),
            Column('fails_raw', INT),
            Column('filtered', INT),
            Column('time_reported', INT),
            Column('request_timestamp', TEXT),
        ], []),
    }
    
    def prepareAcquisition(self):
        try:
            self.save_data = 'no'
            self.warning_limit = float(self.config["input_xml_age_limit"])
            self.filters = self.config["filter"].strip().split(',')
            self.filters_exceptions = self.config['forced_pass'].strip().split(',')
        except KeyError, e:
            raise hf.exceptions.ConfigError('Required parameter "%s" not specified' % str(e))

        if 'blocktest_xml' not in self.config: raise hf.exceptions.ConfigError('blocktest_xml option not set')
        self.blocktest_xml = hf.downloadService.addDownload(self.config['blocktest_xml'])
        self.details_db_value_list = []
        
    def extractData(self):
        data = {'request_timestamp': 0}
        data['source_url'] = self.blocktest_xml.getSourceUrl()
        if self.blocktest_xml.errorOccured() or not self.blocktest_xml.isDownloaded():
            data['error_string'] = 'Source file was not downloaded. Reason: %s' % self.blocktest_xml.error
            data['status'] = -1
            return data
        
        source_tree = etree.parse(open(self.blocktest_xml.getTmpPath()))
        root = source_tree.getroot()
        data["request_date"] = root.get('request_date')
        data["request_timestamp"] = int(float(root.get('request_timestamp')))

        old_data = self.module_table.select().where(self.module_table.c.instance==self.instance_name).order_by(self.module_table.c.request_timestamp.desc()).execute().fetchone()
        if old_data == None:
            self.save_data = 'yes'
            data['data_id'] = 'NULL'
#No old data was found, parse the file and store all values in database:

            num_blocks_raw =0 
            num_files_raw = 0
            num_blocks = 0
            num_files = 0

            for node in root:
                if node.tag == 'node':
                    for block in node:
                        if block.tag == 'block':
                            block_name = block.get('name')
                            block_time_reported = 0
                            akt_files_raw = 0
                            akt_files = 0
                            num_blocks_raw += 1
                            for test in block:
                                if test.tag == 'test':
                                    block_time_reported = int(float(test.get('time_reported')))
                                    for sub_file in test:
                                        if sub_file.tag == 'file':
                                            akt_files_raw +=1
                                            num_files_raw +=1
                                            file_name = sub_file.get('name')
                                            filtered_out=0
                                            for check_reject in self.filters:
                                                if check_reject is not '' and check_reject in file_name:
                                                    filtered_out=1
                                            for check_exception in self.filters_exceptions:
                                                if check_exception is not '' and check_exception in file_name: 
                                                    filtered_out=0
                                            num_files +=1-filtered_out
                                            akt_files +=1-filtered_out
                                            self.details_db_value_list.append({'block': file_name,
                                                                    'isfile':  int(1),
                                                                    'time_reported':  block_time_reported,
                                                                    'fails': num_blocks,
                                                                    'fails_raw': num_blocks_raw,
                                                                    'filtered': filtered_out,
                                                                    })
                            block_filtered_out=1
                            if akt_files>0:
                                num_blocks += 1
                                block_filtered_out=0
                            self.details_db_value_list.append({'block': block_name,
                                                    'isfile': int(0),
                                                    'time_reported': block_time_reported,
                                                    'fails': akt_files,
                                                    'fails_raw': akt_files_raw,
                                                    'filtered':  block_filtered_out,
                                                    })
                                        
            data["failed_blocks_raw"] = num_blocks_raw
            data["failed_blocks"] = num_blocks
            data["failed_total_files_raw"] = num_files_raw
            data["failed_total_files"] = num_files
            
            if data["failed_total_files"] > 0 or data["failed_blocks"] > 0:
                    data['status'] = 0.0
            
        else:
            if old_data['data_id'] != 'NULL':
                old_data = self.module_table.select().where(self.module_table.c.id==old_data['data_id']).execute().fetchone()
            elif old_data['data_id'] == 'NULL':
                pass
            else:
                self.logger.error('Something went terrebly wrong. The dataset, loaded from db was coruppted, file parsed and stored in db')
            if old_data['request_timestamp'] < data['request_timestamp']:
                #The dataset loaded from db is older than the new one, now there is work to do!
                self.save_data = 'yes'
                data['data_id'] = 'NULL'
                num_blocks_raw =0 
                num_files_raw = 0
                num_blocks = 0
                num_files = 0

                for node in root:
                    if node.tag == 'node':
                        for block in node:
                            if block.tag == 'block':
                                block_name = block.get('name')
                                block_time_reported = 0
                                akt_files_raw = 0
                                akt_files = 0
                                num_blocks_raw += 1
                                for test in block:
                                    if test.tag == 'test':
                                        block_time_reported = int(float(test.get('time_reported')))
                                        for sub_file in test:
                                            if sub_file.tag == 'file':
                                                akt_files_raw +=1
                                                num_files_raw +=1
                                                file_name = sub_file.get('name')
                                                filtered_out=0
                                                for check_reject in self.filters:
                                                    if check_reject is not '' and check_reject in file_name:
                                                        filtered_out=1
                                                for check_exception in self.filters_exceptions:
                                                    if check_exception is not '' and check_exception in file_name: 
                                                        filtered_out=0
                                                num_files +=1-filtered_out
                                                akt_files +=1-filtered_out
                                                self.details_db_value_list.append({'block': file_name,
                                                                        'isfile':  int(1),
                                                                        'time_reported':  block_time_reported,
                                                                        'fails': num_blocks,
                                                                        'fails_raw': num_blocks_raw,
                                                                        'filtered': filtered_out,
                                                                        })
                                block_filtered_out=1
                                if akt_files>0:
                                    num_blocks += 1
                                    block_filtered_out=0
                                self.details_db_value_list.append({'block': block_name,
                                                            'isfile': int(0),
                                                            'time_reported': block_time_reported,
                                                            'fails': akt_files,
                                                            'fails_raw': akt_files_raw,
                                                            'filtered':  block_filtered_out,
                                                        })
                                
                data["failed_blocks_raw"] = num_blocks_raw
                data["failed_blocks"] = num_blocks
                data["failed_total_files_raw"] = num_files_raw
                data["failed_total_files"] = num_files
                
                if data["failed_total_files"] > 0 or data["failed_blocks"] > 0:
                    data['status'] = 0.0
            
            elif old_data['request_timestamp'] == data['request_timestamp']:
                # we have old data, just copy them all over
                data['data_id'] = old_data['id']
                data["failed_blocks_raw"] = old_data['failed_blocks_raw']
                data["failed_blocks"] = old_data['failed_blocks']
                data["failed_total_files_raw"] = old_data['failed_total_files_raw']
                data["failed_total_files"] = old_data['failed_total_files']
                data["request_timestamp"] = old_data['request_timestamp']
                data['request_date'] = old_data['request_date']
                data['status'] = old_data['status']
                
            else:
                self.logger.error('Something went terrebly wrong. the timestamp of the old dataset seems to be greater than the new one...')
                data['status'] = -2
                data['data'] = 'NULL'
                data['request_timestamp'] = 0
        return data
        
    def fillSubtables(self, parent_id):
        if self.save_data == 'yes':
            self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])
        
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = []
        if self.dataset['data_id'] != 'NULL':
            details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['data_id']).execute().fetchall()
        else:
            details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['details'] = map(dict, details_list)
        for i, group in enumerate(data['details']):
            group['time_reported'] = datetime.fromtimestamp(group['time_reported'])
        
        return data