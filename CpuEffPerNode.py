# -*- coding: utf-8 -*-

import logging
from sqlalchemy import Column, TEXT, FLOAT
import numpy as np
from operator import attrgetter

import hf
import htcondor

class CpuEffPerNode(hf.module.ModuleBase):
    config_keys = {
            'htcondor_collector': ('The server the HTCondor collector instance is running on.',
                                    'ekpcondorcentral.ekp.kit.edu'),
            'height': ('The height of one cell in the table.', '15'),
            'width': ('The width of one cell.', '7'),
            'cells_per_row': ('The number of cells per row.', '100'),
            'num_threshold': ('Relative number of nodes below eff_threshold above which the status is critical', '0.5'),
            'eff_threshold': ('Efficiency threshold used for the calculation of the status.', '0.5')
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
                 '#AA4499',
                 '#882255'
                 ]

    def prepareAcquisition(self):
        self.logger = logging.getLogger(__name__)
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
        self.htcondor_collector = htcondor.Collector(
                            self.config['htcondor_collector'])


    def extractData(self):
        data = {}
        self.get_node_information()
        self.statistics_db_value_list = self.calculate_efficiency()
        # Determine status of module.
        count = [1 for entry in self.statistics_db_value_list 
                      if entry['efficiency'] <= float(self.config['eff_threshold'])]
        if len(count) >= float(self.config['num_threshold'])*len(self.statistics_db_value_list):
            data['status'] = 0.
        else:
            data['status'] = 1.
        return data

    def get_jobs_from_condor(self):
        htcondor_schedds_ads = self.htcondor_collector.locateAll(htcondor.DaemonTypes.Schedd)
        
        for htcondor_schedd_ad in htcondor_schedds_ads:
            htcondor_schedd = htcondor.Schedd(htcondor_schedd_ad)
            htcondor_jobs = htcondor_schedd.xquery(
                    requirements="JobUniverse =!= 9 && JobStartDate =!= undefined && RemoteHost =!= undefined",
                    projection=self.condor_projection)
            try:
                for htcondor_job in htcondor_jobs:
                    yield htcondor_job
            except RuntimeError as message:
                self.logger.warning('RuntimeError: ' + message)
    
    def get_node_information(self):
        self.node_dict = {}
        nodes = self.htcondor_collector.query(htcondor.AdTypes.Startd)
        for node in nodes:
            name = node['Name'].partition('@')[2].partition('.')[0]
            if name in self.node_dict.keys():
                pass
            else:
                self.node_dict[name] = {
                                'site': node['CloudSite'].lower(),
                                'efficiency': []
                                }

    def calculate_efficiency(self):
        for job in self.get_jobs_from_condor():
            if self.condor_job_status[job['JobStatus']] == 'running':
                cpu_time = job['RemoteUserCpu'] + job['RemoteSysCpu']
                # Catch issues with jobs without JobStartDate ClassAd.
                try:
                    run_time = job['RequestCpus'] * job['ServerTime'] - job['JobStartDate']
                except KeyError:
                    continue
		try:
                    efficiency = float(cpu_time) / float(run_time)
                except ZeroDivisionError:
                    efficiency = 0.
                try:
                    node = job['RemoteHost'].partition('@')[2].partition('.')[0]
                except KeyError:
                    print job['MachineAttrCloudSite0']
                    continue
                if node in self.node_dict.keys():
                    if efficiency <= 1.:
                        self.node_dict[node]['efficiency'].append(efficiency)
                else:
                    if efficiency <= 1.:
                        self.node_dict[node] = {
                                'site': job['MachineAttrCloudSite0'].lower(),
                                'efficiency': [efficiency]
                                }
        node_list = []
        # Build a list of the dictionaries and add the node in that dictionary.
        for node in self.node_dict.keys():
            if len(self.node_dict[node]['efficiency']) > 0:
                self.node_dict[node]['efficiency'] = round(
                        100*np.mean(self.node_dict[node]['efficiency']),2)
            else:
                self.node_dict[node]['efficiency'] = 0
            keys = self.node_dict[node].keys()
            values = self.node_dict[node].values()
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
                            order_by(self.subtables['statistics'].c.site.asc(),
                                     self.subtables['statistics'].c.node.asc()).\
                            execute().fetchall()
        # Create list that contains dicts {x, y, node, efficiency, color}
        data_list = []
        # A list containing sites that have been translated to plot objects.
        sites_list = []
        # Map the site to an y value.
        site_mapper = [] 
        # Save y value for each line.
        lines = []
        y = -1 

        for values in nodes_list:
            if values['site'] not in sites_list:
                sites_list.append(values['site'])
                # Leave two lines empty and start from the left.
                y += 3
                x = 0
                site_mapper.append({'site': values['site'], 'y': y})
                lines.append(y-2)
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
                    'color': self.color_map[int(values['efficiency'])/12] if \
                                                values['efficiency'] > 0 else \
                                                '#FFFFFF'                                  
                    }
            data_list.append(help_dict)
        
        lines.append(y+1)
        data['node_list'] = data_list
        data['sites'] = site_mapper
        data['height'] = y * int(self.config['height']) + 100
        data['width'] = int(self.config['width']) 
        data['color'] = self.color_map
	data['lines'] = lines

        return data
