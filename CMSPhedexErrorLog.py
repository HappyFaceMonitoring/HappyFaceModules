# -*- coding: utf-8 -*-
import hf, lxml, logging, datetime
from sqlalchemy import *
from lxml import etree

#Alles in Class umschreiben! wichtig: bewertung des moduls + der globalerrors in acquire auslesen und darauf achten, dass eingestellt werden kann ob es das to oder from  modul ist! Datetimeabfrage fuer die einzelnen errors in den files mit grundtimestamp von request der page inklusive einstellbar wie weit die fehler zeitlich davon abweichen duerfen
class CMSPhedexErrorLog(hf.module.ModuleBase):
    
    config_keys = {
        'link_direction': ("""represents the way of the links, 'from' means the file comes from somewhere and goes to KIT
'to' means the file comes from KIT and goes to somewhere else
this is necessary to determine destination or source of the file!""", 'from'),
        'timerange_seconds': ('Ignore errors that are older than the specified time', '3600'),
        'source_url': ('', ''),
        'min_error': ('minimal number of errors needed to determine status', '50'),
        'warning_dest': ('25% = 25', '25'),
        'critical_dest': ('25% = 25', '50'),
        'warning_source': ('25% = 25', '100'),
        'critical_source': ('25% = 25', '100'),
        'warning_trans': ('25% = 25', '100'),
        'critical_trans': ('25% = 25', '100'),
    }
    config_hint = ''
    
    table_columns = [
        Column('summary', FLOAT),
        Column('transfer', FLOAT),
        Column('destination', FLOAT),
        Column('source', FLOAT),
        Column('unknown', FLOAT),
        Column('frac_dest', FLOAT),
        Column('frac_source', FLOAT),
        Column('frac_trans', FLOAT),
        Column('frac_unknown', FLOAT),
        Column('transfer_status', TEXT),
        Column('destination_status', TEXT),
        Column('source_status', TEXT),
        Column('unknown_status', TEXT),
    ], []
    
    subtable_columns = {
        'details': ([
            Column('node', TEXT),
            Column('transfer', FLOAT),
            Column('destination', FLOAT),
            Column('source', FLOAT),
            Column('unknown', FLOAT),
            Column('trans_message', TEXT),
            Column('dest_message', TEXT),
            Column('source_message', TEXT),
            Column('unknown_message', TEXT)
        ], [])
    }

    
    def prepareAcquisition(self):
        try:
            self.link_direction = self.config['link_direction']
            self.timerange_seconds = int(self.config['timerange_seconds'])
            self.source = hf.downloadService.addDownload(self.config['source_url'])
            self.min_error = float(self.config['min_error'])
            self.warning_dest = float(self.config['warning_dest'])
            self.critical_dest = float(self.config['critical_dest'])
            self.warning_source = float(self.config['warning_source'])
            self.critical_source = float(self.config['critical_source'])
            self.warning_trans = float(self.config['warning_trans'])
            self.critical_trans = float(self.config['critical_trans'])
        
        except KeyError, ex:
            raise hf.exceptions.ConfigError('Required parameter "%s" not specified' % str(ex))
            
        self.details_db_value_list = []
    
    def extractData(self):

        data = {}
        data['destination'] = 0
        data['source'] = 0
        data['transfer'] = 0
        data['unknown'] = 0
        data['summary'] = 0
        sourcedata = {}
        data['source_url'] = self.source.getSourceUrl()
        if self.source.errorOccured() or not self.source.isDownloaded():
            data['error_string'] = 'Source file was not downloaded. Reason: %s' % self.source.error
            data['status'] = -1
            return data
            
        source_tree = etree.parse(open(self.source.getTmpPath()))

        
        root = source_tree.getroot()
        request_time = root.get('request_timestamp')
        for link in root:
            if link.tag == 'link':
                sourcedata[link.get(self.link_direction)] = []
                for block in link:
                    for file in block: 
                        if file.tag == 'file':
                            sourcedata[link.get(self.link_direction)].append(file)
        
        for site, files in sourcedata.iteritems():
            stash = {}
            stash['node'] = site
            stash['trans_message'] = 'Not set'
            stash['dest_message'] = 'Not set'
            stash['source_message'] = 'Not set'
            stash['unknown_message'] = 'Not set'
            transfer = 0
            source = 0
            destination = 0
            unknown = 0
            for file in files:
                atransfer = 0
                asource = 0
                adestination = 0
                aunknown = 0
                for errors in file:
                    if abs(float(request_time) - float(errors.get('time_done'))) <= self.timerange_seconds:
                        for details in errors:
                            if details.tag == 'detail_log':
                                tempstring = details.text.split(' ')
                                if tempstring[0] == 'TRANSFER' and atransfer == 0:
                                    transfer += 1
                                    atransfer = 1
                                    stash['trans_message'] = str(details.text)
                                elif tempstring[0] == 'DESTINATION' and adestination == 0:
                                    destination += 1
                                    adestination = 1
                                    stash['dest_message'] = str(details.text)
                                elif tempstring[0] == 'SOURCE' and asource == 0:
                                    source += 1
                                    asource = 1
                                    stash['source_message'] = str(details.text)
                                elif aunknown == 0:
                                    unknown += 1
                                    aunknown = 1
                                    stash['unknown_message'] = str(details.text)
            if transfer != 0 or source != 0 or destination !=0 or unknown != 0:
                stash['transfer'] = transfer
                stash['destination'] = destination
                stash['source'] = source
                stash['unknown'] = unknown
                data['destination'] += destination
                data['source'] += source
                data['transfer'] += transfer
                data['unknown'] += unknown
                self.details_db_value_list.append(stash)
        
        data['status'] = 1.0
        data['destination_status'] = 'ok'
        data['source_status'] = 'ok'
        data['transfer_status'] = 'ok'
        data['unknown_status'] = 'ok'
        data['summary'] = data['source'] + data['unknown'] + data['transfer'] + data['destination']
        
        if float(data['summary']) > 0:
            data['frac_dest'] = float(data['destination'] * 100 / data['summary']) 
            if data['frac_dest'] >= self.warning_dest:
                data['destination_status'] = 'warning'
                data['status'] = 0.5
            elif data['frac_dest'] >= self.critical_dest:
                data['destination_status'] = 'critical'
                data['status'] = 0.0
            
            data['frac_source'] = float(data['source'] * 100 / data['summary'])
            if data['frac_source'] >= self.warning_source:
                data['source_status'] = 'warning'
                data['status'] = 0.5
            elif data['frac_source'] >= self.critical_source:
                data['source_status'] = 'critical'
                data['status'] = 0.0
                
            data['frac_trans'] = float(data['transfer'] * 100 / data['summary'])   
            if data['frac_trans'] >= self.warning_trans:
                data['transfer_status'] = 'warning'
                data['status'] = 0.5
            elif data['frac_trans'] >= self.critical_trans:
                data['transfer_status'] = 'critical'
                data['status'] = 0.0
            
            data['frac_unknown'] = float(data['unknown'] * 100 / data['summary'])
        
        
        return data
        
    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])
        
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['details'] = map(dict, details_list)
        return data
