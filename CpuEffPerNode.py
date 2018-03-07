# -*- coding: utf-8 -*-

import logging
from sqlalchemy import Column, TEXT, FLOAT
import numpy as np

import hf
import htcondor

class CpuEffPerNode(hf.module.ModuleBase):
    config_keys = {
            'htcondor_collector': ('The server the HTCondor collector instance is running on.',
                                    'ekpcondorcentral.ekp.kit.edu'),
            'height': ('The height of one cell in the table.', '15'),
            'width': ('The width of one cell.', '7'),
            'cells_per_row': ('The number of cells per row.', '100')
    }
    table_columns = [], []
    subtable_columns = {
            'statistics': ([
                Column('node', TEXT),
                Column('site', TEXT),
                Column('efficiency', FLOAT)], [])
            }
    color_map = ['#332288', 
                 '#88CCEE',
                 '#44AA99', 
                 '#117733',
                 '#999933',
                 '#DDCC77', 
                 '#CC6677',
                 '#882255',
                 '#AA4499'
                 ]

    def prepareAcquisition(self):
        self.source_url = self.config['htcondor_collector']
        self.condor_projection = [
                'RemoteHost',
                'ServerTime',
                'JobStartDate',
                'JobCurrentStartDate',
                'MachineAttrCloudSite0',
                'RemoteUserCpu',
                'RemoteSysCpu',
                'RequestCpus',
                'JobStatus'
                ]

        self.condor_job_status = {
                0: 'unexpanded',
                1: 'idle',
                2: 'running',
                3: 'removed',
                4: 'completed',
                5: 'held',
                6: 'submission_er',
                7: 'suspended'}


    def extractData(self):
        data = {}
        self.statistics_db_value_list = self.calculate_efficiency()
        return data

    def get_jobs_from_condor(self, htcondor_collector_host):
        htcondor_collector = htcondor.Collector(htcondor_collector_host)
        htcondor_schedds_ads = htcondor_collector.locateAll(htcondor.DaemonTypes.Schedd)
        
        for htcondor_schedd_ad in htcondor_schedds_ads:
            htcondor_schedd = htcondor.Schedd(htcondor_schedd_ad)
            htcondor_jobs = htcondor_schedd.xquery(
                    requirements="JobUniverse =!= 9",
                    projection=self.condor_projection)
            for htcondor_job in htcondor_jobs:
                yield htcondor_job

    def calculate_efficiency(self):
        node_dict = {}
        for job in self.get_jobs_from_condor(self.config['htcondor_collector']):
            if self.condor_job_status[job['JobStatus']] == 'running':
                cpu_time = job['RemoteUserCpu'] + job['RemoteSysCpu']
                run_time = job['RequestCpus'] * job['ServerTime'] - job['JobStartDate']
		try:
                    efficiency = float(cpu_time) / float(run_time)
                except ZeroDivisionError:
                    efficiency = 0.
                try:
                    node = job['RemoteHost'].partition('@')[2].partition('.')[0]
                except KeyError:
                    print job['MachineAttrCloudSite0']
                    continue
                if node in node_dict.keys():
                    if efficiency <= 1.:
                        node_dict[node]['efficiency'].append(efficiency)
                else:
                    if efficiency <= 1.:
                        node_dict[node] = {
                                'site': job['MachineAttrCloudSite0'].lower(),
                                'efficiency': [efficiency]
                                }
        node_list = []
        # Build a list of the dictionaries and add the node in that dictionary.
        for node in node_dict.keys():
            if len(node_dict[node]['efficiency']) > 0:
                node_dict[node]['efficiency'] = round(
                        100*np.mean(node_dict[node]['efficiency']),2)
            else:
                node_dict[node]['efficiency'] = 0
            keys = node_dict[node].keys()
            values = node_dict[node].values()
            keys.append('node')
            values.append(node)
            node_list.append(dict(zip(keys,
                                    values)))
        return node_list

    def fillSubtables(self, parent_id):
        self.subtables['statistics'].insert().execute(
                [dict(parent_id=parent_id, **row) for row in self.statistics_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        # Get dicts of subtable entries ordered by site.
        nodes_list = self.subtables['statistics'].select().\
                            where(self.subtables['statistics'].c.parent_id \
                            == self.dataset['id']).\
                            order_by(self.subtables['statistics'].c.site.asc()).\
                            execute().fetchall()
        # Create list that contains dicts {x, y, node, efficiency, color}
        data_list = []
        # A list containing sites that have been translated to plot objects.
        sites_list = []
        # Map the site to an y value.
        site_mapper = [] 
        y = 0

        for values in nodes_list:
            if values['site'] not in sites_list:
                sites_list.append(values['site'])
                # Leave to lines empty and start from the left.
                y += 2
                x = 0
                site_mapper.append({'site': values['site'], 'y': y})
            else:
                # Move one unit right.
                x += 1
            if x == int(self.config['cells_per_row']):
                # Jump to next line and start from the left.
                y += 1
                x = 0
            help_dict = {
                    'x': x,
                    'y': y,
                    'node': values['node'],
                    'efficiency': values['efficiency'],
                    'color': self.color_map[int(values['efficiency'])/12]
                    }
            data_list.append(help_dict)
        
        data['node_list'] = data_list
        data['sites'] = site_mapper
        data['height'] = y * int(self.config['height']) + 100
        data['width'] = int(self.config['width']) 

        return data
