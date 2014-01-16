# -*- coding: utf-8 -*-
import hf, logging, datetime
import parser
from sqlalchemy import *
from lxml import etree


class Ganglia(hf.module.ModuleBase):
    config_keys = {
        'ganglia_xml': ('link to the source file', ''),
    }

    config_hint = ''

#comment unneccesary ones out
    table_columns = [
#        Column('machine_type', TEXT),
#        Column('os_release', TEXT),
        Column('bytes_out', FLOAT),
#        Column('amga_state_time_wait', FLOAT),
#        Column('cpu_idle', FLOAT),
#        Column('mem_buffers', FLOAT),
#        Column('part_max_used', FLOAT),
#        Column('amga_state_time_wait_res', FLOAT),
        Column('bytes_out_res', FLOAT),
        Column('disk_free', FLOAT),
        Column('disk_free_res', FLOAT),
#        Column('cpu_nice', FLOAT),
#        Column('cpu_nice_res', FLOAT),
#        Column('cpu_speed', INT),
#        Column('machine_type_1', TEXT),
        Column('bytes_out_res_1', FLOAT),
        Column('bytes_out_1', FLOAT),
#        Column('cpu_speed_res', FLOAT),
#        Column('cpu_idle_res', FLOAT),
        Column('disk_free_1', FLOAT),
        Column('disk_free_res_1', FLOAT),
#        Column('cpu_nice_1', FLOAT),
#        Column('cpu_nice_res_1', FLOAT),
#        Column('cpu_speed_1', INT),
#        Column('cpu_speed_res_1', FLOAT),
#        Column('cpu_idle_1', FLOAT),    
#        Column('cpu_idle_res_1', FLOAT),
        Column('load_fifteen', FLOAT),
        Column('load_fifteen_res', FLOAT),
        Column('load_fifteen_1', FLOAT),
        Column('load_fifteen_res_1', FLOAT),
    ], []


    def prepareAcquisition(self):
        self.source_url = 'www.google.com'
        # read configuration
        if 'ganglia_xml' not in self.config: raise hf.exceptions.ConfigError('ganglia_xml option not set')
        self.ganglia_xml = hf.downloadService.addDownload(self.config['ganglia_xml'])


        # ..._res for results
    def extractData(self):

        data = {'source_url': self.ganglia_xml.getSourceUrl(),
                'status': -1,
                'LOCALTIME': '',
#                'machine_type': '',
                'bytes_out': 0,
                'bytes_out_res': 0.0,
#                'machine_type_1': '',
                'bytes_out_1': 0,
                'bytes_out_res_1': 0.0,
#                'amga_state_time_wait': '',
#                'amga_state_time_wait_res': 0.0,
#                'cpu_idle': '',
#                'cpu_idle_res': 0.0,
#                'mem_buffers': '',
#                'part_max_used': '',
                'disk_free': 0,
                'disk_free_res': 0,
#                'cpu_nice': '',
#                'cpu_nice_res': '',
#                'cpu_speed': '',
#                'cpu_speed_res': '',
                'disk_free_1': 0,
                'disk_free_res_1': 0,
 #               'cpu_nice_1': '',
 #               'cpu_nice_res_1': '',
 #               'cpu_speed_1': '',
 #               'cpu_speed_res_1': '',
 #               'cpu_idle_1': '',
 #               'cpu_idle_res_1': 0.0,
 #               'aCu': '',
 #               'os_release': '',
                 'load_fifteen': 0,
                 'load_fifteen_res': 0.0,
                 'load_fifteen_1': 0,
                 'load_fifteen_res_1': 0.0}
                
        #source_tree = etree.parse(open(self.ganglia_xml.getTmpPath()))
        source_tree = etree.parse(open('/home/happy/HappyFace/gangliatestdata'))
        root = source_tree.getroot()

        self.GRID_found = False
        self.status = -1
        for sec in root.findall("GRID"):
                self.LOCALTIME = int(sec.get('LOCALTIME'))

        


                #every layer with METRIC in the beginning
                #test logic (status) in the if-statements where the ..._res variables are set

        #can68 is AMGA Server
        for fifth in root.findall(".//*[@NAME='can68.cc.kek.jp']/METRIC"):
                       
                        self.GRID_found = True
                        
#                        if fifth.get('NAME') == "amga.Connections.used":
#                            self.aCu = fifth.get('VAL')
                            
#                        if fifth.get('NAME') == "os_release":
#                            self.os_release = fifth.get('VAL')
                            
#                        if fifth.get('NAME') == "machine_type":
#                            self.machine_type = fifth.get('VAL')
                            
                        if fifth.get('NAME') == "bytes_out":
                            self.bytes_out = float(fifth.get('VAL'))
                            if self.bytes_out <= 100000:
                                self.bytes_out_res = 0.5
                                if self.status != 0.0:
                                    self.status = 0.5
                            else:
                                self.bytes_out_res = 1.0                                
                            
#                        if fifth.get('NAME') == "amga-state.time_wait":
#                            self.amga_state_time_wait = fifth.get('VAL')
#                            if self.amga_state_time_wait > 0:
#                                self.amga_state_time_wait_res = 0.5
#                                if self.status != 0.0:
#                                    self.status = 0.5
#                            else:
#                                self.amga_state_time_wait_res = 1.0
                                
                        if fifth.get('NAME') == "load_fifteen":
                            self.load_fifteen = float(fifth.get('VAL'))
                            load_limit = 0.5
                            if self.load_fifteen >= load_limit:
                                self.load_fifteen_res = 0.5
                                if self.status != 0.0:
                                    self.status = 0.5
                            elif self.load_fifteen >= 0.75:
                                self.load_fifteen_res = 0.0
                                self.status = 0.0
                            else:
                                self.load_fifteen_res = 1.0
                                                            
#                        if fifth.get('NAME') == "cpu_idle":
#                            self.cpu_idle = fifth.get('VAL')
#                            if self.cpu_idle > 80:
#                                self.cpu_idle_res = 0.5
#                                if self.status != 0.0:
#                                    self.status=0.5
#                            else:
#                                self.cpu_idle_res = 1.0
                            
#                        if fifth.get('NAME') == "mem_buffers":
#                            self.mem_buffers = fifth.get('VAL')

#                        if fifth.get('NAME') == "part_max_used":
#                            self.part_max_used = fifth.get('VAL')

                        if fifth.get('NAME') == "disk_free":
                            self.disk_free = float(fifth.get('VAL'))
                            if self.disk_free <= 100:
                                self.disk_free_res = 0.5
                                if self.status != 0.0:
                                    self.status = 0.5
                            elif self.disk_free <= 50:
                                self.disk_free_res = 0.0
                                self.status = 0.0
                            else:
                                self.disk_free_res = 1.0        
                                

#                        if fifth.get('NAME') == "cpu_nice":
#                            self.cpu_nice = fifth.get('VAL')
#                            if self.cpu_nice > 10:
#                                self.cpu_nice_res = 0.5
#                                if self.status != 0.0:
#                                    self.status = 0.5
#                            else:
#                                self.cpu_nice_res = 1.0        

#                        if fifth.get('NAME') == "cpu_speed":
#                            self.cpu_speed = fifth.get('VAL')
#                            if self.cpu_speed < 3000:
#                                self.cpu_speed_res = 0.5
#                                if self.status != 0.0:
#                                    self.status = 0.5
#                            else:
#                                self.cpu_speed_res = 1.0        

        #can61 is DIRAC Server --> variables end with ..._1
        for sixth in root.findall(".//*[@NAME='can61.cc.kek.jp']/METRIC"):
                       
                        self.GRID_found = True
                        
#                        if sixth.get('NAME') == "machine_type":
#                            self.machine_type_1 = sixth.get('VAL')
                            
                        if sixth.get('NAME') == "bytes_out":
                            self.bytes_out_1 = float(sixth.get('VAL'))
                            if self.bytes_out_1 <= 100000:
                                self.bytes_out_res_1 = 0.5
                                if self.status != 0.0:
                                    self.status = 0.5
                            else:
                                self.bytes_out_res_1 = 1.0                                


                        if sixth.get('NAME') == "disk_free":
                            self.disk_free_1 = float(sixth.get('VAL'))
                            if self.disk_free_1 <= 100:
                                self.disk_free_res_1 = 0.5
                                if self.status != 0.0:
                                    self.status = 0.5
                            elif self.disk_free_1 <= 50:
                                self.disk_free_res_1 = 0.0
                                self.status = 0.0
                            else:
                                self.disk_free_res_1 = 1.0                                
                                
                        if sixth.get('NAME') == "load_fifteen":
                            self.load_fifteen_1 = float(sixth.get('VAL'))
                            if self.load_fifteen_1 >= 0.5:
                                self.load_fifteen_res_1 = 0.5
                                if self.status != 0.0:
                                    self.status = 0.5
                            elif self.load_fifteen_1 >= 0.75:
                                self.load_fifteen_res_1 = 0.0
                                self.status = 0.0
                            else:
                                self.load_fifteen_res_1 = 1.0                                

#                        if sixth.get('NAME') == "cpu_nice":
#                            self.cpu_nice_1 = sixth.get('VAL')
#                            if self.cpu_nice_1 > 10:
#                                self.cpu_nice_res_1 = 0.5
#                                if self.status != 0.0:
#                                    self.status = 0.5
#                            else:
#                                self.cpu_nice_res_1 = 1.0        

#                        if sixth.get('NAME') == "cpu_speed":
#                            self.cpu_speed_1 = sixth.get('VAL')
#                            if self.cpu_speed_1 < 3000:
#                                self.cpu_speed_res_1 = 0.5
#                                if self.status != 0.0:
#                                    self.status = 0.5
#                            else:
#                                self.cpu_speed_res_1 = 1.0
                                
#                        if sixth.get('NAME') == "cpu_idle":
#                            self.cpu_idle_1 = sixth.get('VAL')
#                            if self.cpu_idle_1 > 80:
#                                self.cpu_idle_res_1 = 0.5
#                                if self.status != 0.0:
#                                    self.status=0.5
#                            else:
#                                self.cpu_idle_res_1 = 1.0        


                            
        if not self.GRID_found:
            data['error_string'] = 'HOST was not found in data source.'
            data['status'] = -1
            return data
            
        # definition of the database table values
        data['LOCALTIME'] = self.LOCALTIME
#        data['machine_type'] = self.machine_type
        data['bytes_out'] = self.bytes_out
#        data['amga_state_time_wait'] = self.amga_state_time_wait
#        data['cpu_idle'] = self.cpu_idle
#        data['mem_buffers'] = self.mem_buffers
#        data['part_max_used'] = self.part_max_used
        data['bytes_out_res'] = self.bytes_out_res
#        data['amga_state_time_wait_res'] = self.amga_state_time_wait_res
#        data['cpu_idle_res'] = self.cpu_idle_res
#        data['os_release'] = self.os_release
        data['status'] = self.status
        data['disk_free'] = self.disk_free
        data['disk_free_res'] = self.disk_free_res
#        data['cpu_nice'] = self.cpu_nice
#        data['cpu_nice_res'] = self.cpu_nice_res
#        data['cpu_speed'] = self.cpu_speed
#        data['cpu_speed_res'] = self.cpu_speed_res
#        data['machine_type_1'] = self.machine_type_1
        data['bytes_out_1'] = self.bytes_out_1
        data['bytes_out_res_1'] = self.bytes_out_res_1
        data['disk_free_1'] = self.disk_free_1
        data['disk_free_res_1'] = self.disk_free_res_1
#        data['cpu_nice_1'] = self.cpu_nice_1
#        data['cpu_nice_res_1'] = self.cpu_nice_res_1
#        data['cpu_speed_1'] = self.cpu_speed_1
#        data['cpu_speed_res_1'] = self.cpu_speed_res_1
#        data['cpu_idle_1'] = self.cpu_idle_1
#        data['cpu_idle_res_1'] = self.cpu_idle_res_1
        data['load_fifteen'] = self.load_fifteen
        data['load_fifteen_res'] = self.load_fifteen_res
        data['load_fifteen_1'] = self.load_fifteen_1
        data['load_fifteen_res_1'] = self.load_fifteen_res_1
        
        return data
