# -*- coding: utf-8 -*-
import hf, logging, datetime
from sqlalchemy import *
from lxml import etree

class GangliaGoe(hf.module.ModuleBase):
    config_keys = {
        'source_url': ('Link to the Ganglia XML file', ''),
        'hosts': ('Comma-separated list of hosts to monitor', ''),
        'cluster': ('Name of the cluster to monitor', 'GoeGrid_Server'),
    }

    config_hint = ''

#comment unneccesary ones out
    table_columns = [
        Column('source_url', TEXT),
    ], []

    subtable_columns = {
        'host_details': ([
        Column('host', TEXT),
        Column('variable', TEXT),
        Column('value', TEXT),
        Column('unit', TEXT),
        Column('info', TEXT),
    ], [])}

    def prepareAcquisition(self):
        self.source_url = self.config['source_url']
        self.hostlist = self.config['hosts'].split(',')
        for index, host in enumerate(self.hostlist):
            self.hostlist[index] = host.strip()
        self.source = hf.downloadService.addDownload(self.config['source_url'])

    def extractData(self):
        self.details_db_value_list = []
        data = {'source_url': self.source_url,
                'status': 0,
               }
        
        xmlroot = etree.fromstring(open(self.source.getTmpPath()).read())

        cluster_found = False
        for cluster in xmlroot.findall('CLUSTER'):
            if cluster.get('NAME') != self.config['cluster']:
                continue
            else:
                cluster_found = True
                for host in cluster.findall('HOST'):
                    if host.get('NAME') in self.hostlist:
                        for metric in host.findall('METRIC'):
                            info = ''
                            for extradata in metric.findall('EXTRA_DATA'):
                                for extraelement in extradata.findall('EXTRA_ELEMENT'):
                                    if extraelement.get('NAME') == 'TITLE':
                                        info = extraelement.get('VAL')
                            self.details_db_value_list.append({'host': host.get('NAME'), 'variable': metric.get('NAME'), 'value': metric.get('VAL'), 'unit': metric.get('UNITS'), 'info': info})

        if cluster_found == False:
            data['status'] = -1

        #print self.details_db_value_list

        return data

    def fillSubtables(self, parent_id):
        self.subtables['host_details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        host_details = self.subtables['host_details'].select().where(self.subtables['host_details'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['host_details'] = map(dict, host_details)
        hostlist = self.config['hosts'].split(',')
        for index, host in enumerate(hostlist):
            hostlist[index] = host.strip()
        data['hostlist'] = hostlist
        return data
