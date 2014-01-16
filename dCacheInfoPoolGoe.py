# -*- coding: utf-8 -*-
#
# Copyright 2012 Institut fÃ¼r Experimentelle Kernphysik - Karlsruher Institut fÃ¼r Technologie
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

import hf, lxml, logging, datetime
from sqlalchemy import *
from xml.dom import minidom
from lxml.html import parse
from string import strip
from string import replace
import parser

class dCacheInfoPoolGoe(hf.module.ModuleBase):
    config_keys = {
        'global_critical_ratio': ('ratio determines module status: (sum of free space)/(sum of total space)', '0.1'),
        'local_critical_ratio': ('ratio determines pool status: pool free/total', '0.02'),
        'global_warning_ratio': ('ratio determines module status: (sum of free space)/(sum of total space)', '0.15'),
        'local_warning_ratio': ('ratio determines pool status: pool free/total', '0.05'),
        'global_critical_poolcriticals': ('module status is critical if more than this amount of pools are  critical pools', '1'),
        'global_critical_poolwarnings': ('module status is critical if more than this amount of pools are  warning pools', '4'),
        'global_warning_poolcriticals': ('module status is warning if more than this amount of pools are  critical pools', '0'),
        'global_warning_poolwarnings': ('module status is warning if more than this amount of pools are  warning pools', '0'),
        'poolgroups': ('name of the pools, a list is possible', 'rT_ops, rT_cms'),
        'unit': ('This should be GiB or TiB', 'TiB'),
        'source_xml': ('link to the source file', 'both||http://adm-dcache.gridka.de:2286/info/pools'),
        'special_overview': ('this parameter allows you to add several new lines to the overview, you have 4 variables(total, free, precious, removable) you can use to define the new line. this adds the line example with the value calculated the way described after =', 'example[%]=(r+t)/(f-p)*100'),
        'special_details': ('it is equal to special_overview but adds a new column for details', 'example=(r+t)/(f-p)'),
    }
    #'categories': ('name of the categories to be extracted, poolname and status will always be generated', 'total,free,precious,removable'),
    config_hint = 'For dCache version 1.9'

    table_columns = [
        Column('num_pools', INT),
        Column('crit_pools', INT),
        Column('warn_pools', INT),
        Column('total', INT),
        Column('free', INT),
        Column('precious', INT),
        Column('removable', INT),
        Column('special_overview', TEXT),
        Column('special_details', TEXT),
        Column('unit', TEXT),
    ], []

    subtable_columns = {
        "details": ([
            Column('poolname', TEXT),
            Column('total', FLOAT),
            Column('free', FLOAT),
            Column('precious', FLOAT),
            Column('removable', FLOAT),
            Column('status', FLOAT),
        ], []),
    }


    def prepareAcquisition(self):
        self.source_url = 'both||www.google.com'
        # read configuration
        try:
            self.global_critical_ratio = float(self.config['global_critical_ratio'])
            self.local_critical_ratio = float(self.config['local_critical_ratio'])
            self.global_warning_ratio = float(self.config['global_warning_ratio'])
            self.local_warning_ratio = float(self.config['local_warning_ratio'])
            self.global_critical_poolcriticals = int(self.config['global_critical_poolcriticals'])
            self.global_critical_poolwarnings = int(self.config['global_critical_poolwarnings'])
            self.global_warning_poolcriticals = int(self.config['global_warning_poolcriticals'])
            self.global_warning_poolwarnings = int(self.config['global_warning_poolwarnings'])
            self.poolgroups = map(strip, self.config['poolgroups'].split(','))
            self.unit = self.config['unit']
            self.special_overview = self.config['special_overview']
            self.special_details = self.config['special_details']
        except KeyError, e:
            raise hf.exceptions.ConfigError('Required parameter "%s" not specified' % str(e))

        if 'source_xml' not in self.config: raise hf.exceptions.ConfigError('source_xml option not set')
        self.source_xml = hf.downloadService.addDownload(self.config['source_xml'])
        self.source_url = self.source_xml.getSourceUrl()
        self.details_db_value_list = []

    def extractData(self):
        data = {}
        if self.unit != 'GiB' and self.unit != 'TiB':
            self.logger.error(self.unit + ' is not an accepted unit, using TiB instead!')
            self.unit = 1024 * 1024 * 1024 * 1024
            data['unit'] = 'TiB'
        elif self.unit == 'GiB':
            self.unit = 1024 * 1024 * 1024
            data['unit'] = 'GiB'
        else:
            self.unit = 1024 * 1024 * 1024 * 1024
            data['unit'] = 'TiB'
        data['status'] = 1
        data['special_overview'] = self.special_overview
        data['special_details'] = self.special_details

        # parse xml (old dCache versions, e.g. 1.9)
        xmldoc = minidom.parse(open(self.source_xml.getTmpPath()))
        for pool in xmldoc.getElementsByTagName('pool'):
            groups = pool.getElementsByTagName('poolgroups')
            for group in groups:
                refs = group.getElementsByTagName('poolgroupref')
                for ref in refs:
                    spaces = pool.getElementsByTagName('space')
                    for space in spaces:
                        metrics = space.getElementsByTagName('metric')
                        metrics_values = {}
                        for metric in metrics:
                            if metric.getAttribute('name') == 'total':
                                metrics_values['total'] = float(metric.firstChild.nodeValue)
                            elif metric.getAttribute('name') == 'free':
                                metrics_values['free'] = float(metric.firstChild.nodeValue)
                            elif metric.getAttribute('name') == 'precious':
                                metrics_values['precious'] = float(metric.firstChild.nodeValue)
                            elif metric.getAttribute('name') == 'removable':
                                metrics_values['removable'] = float(metric.firstChild.nodeValue)
                            else:
                                pass
                    if ref.getAttribute('name') in self.poolgroups:
                        append = {'poolname': pool.getAttribute('name'), 'total': metrics_values['total']/self.unit, 'free': metrics_values['free']/self.unit, 'precious': metrics_values['precious']/self.unit, 'removable': metrics_values['removable']/self.unit}
                        self.details_db_value_list.append(append)
                        break


        data['num_pools'] = 0
        data['crit_pools'] = 0
        data['warn_pools'] = 0
        data['total'] = 0
        data['free'] = 0
        data['precious'] = 0
        data['removable'] = 0        
        for pool in self.details_db_value_list:
            data['num_pools'] += 1
            data['total'] += pool['total']
            data['free'] += pool['free']
            data['precious'] += pool['precious']
            data['removable'] += pool['removable']

            if pool['total'] == 0:
                pool['status'] = 0.0
                data['crit_pools'] += 1
            elif (pool['free'] + pool['removable']) / pool['total'] <= self.local_critical_ratio:
                pool['status'] = 0.0
                data['crit_pools'] += 1
            elif (pool['free'] + pool['removable']) / pool['total'] <= self.local_warning_ratio:
                pool['status'] = 0.5
                data['warn_pools'] += 1
            else:
                pool['status'] = 1.0

        if (data['free'] + data['removable']) / data['total'] <= self.global_critical_ratio or data['crit_pools'] > self.global_critical_poolcriticals or data['warn_pools'] > self.global_critical_poolwarnings:
            data['status'] = 0.0
        elif (data['free'] + data['removable']) / data['total'] <= self.global_warning_ratio or data['crit_pools'] > self.global_warning_poolcriticals or data['warn_pools'] > self.global_warning_poolwarnings:
            data['status'] = 0.5

        return data

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).execute().fetchall()
        details_list = map(lambda x: dict(x), details_list)
        
        try:
            special_overview = self.dataset['special_overview'].split(',')
        except AttributeError:
            special_overview = []
        try:
            special_details = self.dataset['special_details'].split(',')
        except AttributeError:
            special_details = []
            
        for i,special in enumerate(special_overview):
            if '=' in special:
                special_overview[i] = special.split('=')
                special_overview[i][1] = parser.expr(special_overview[i][1]).compile()
            else:
                special_overview = None

        for i,special in enumerate(special_details):
            if '=' in special:
                special_details[i] = special.split('=')
                special_details[i].append(parser.expr(special_details[i][1]).compile())
            else:
                special_details = None

        overview_list = []
        t = self.dataset['total']
        p = self.dataset['precious']
        f = self.dataset['free']
        r = self.dataset['removable']
        overview_list.append(['Pools', self.dataset['num_pools']])
        overview_list.append(['Pools with status warning', self.dataset['warn_pools']])
        overview_list.append(['Pools with status critical', self.dataset['crit_pools']])
        overview_list.append(['Pools with status warning [%]', float(self.dataset['warn_pools']) / self.dataset['num_pools']*100])
        overview_list.append(['Pools with status critical [%]', float(self.dataset['crit_pools']) / self.dataset['num_pools']*100])
        overview_list.append(['Total Space [' + self.dataset['unit'] + ']', '%.2f' %t])
        overview_list.append(['Free Space [' + self.dataset['unit'] + ']', '%.2f' %f])
        overview_list.append(['Used Space [' + self.dataset['unit'] + ']', '%.2f' %(t - f)])
        overview_list.append(['Precious Space [' + self.dataset['unit'] + ']', '%.2f' %p])
        overview_list.append(['Removable Space [' + self.dataset['unit'] + ']', '%.2f' %r])

        if special_overview is not None:
            for i,special in enumerate(special_overview):
                try:
                    overview_list.append([special[0], eval(special[1])])
                except ValueError:
                    overview_list.append([special[0], 'matherror'])
                except TypeError:
                    overview_list.append([special[0], 'typeerror'])
        details_finished_list = []
        help_appending = []
        help_appending.append('none')
        help_appending.append('Poolname')
        help_appending.append("<input type='checkbox' id='" +self.dataset['instance'] + "_variable_0' value='total' checked='checked' />" + 'Total Space [' + self.dataset['unit'] + ']')
        help_appending.append("<input type='checkbox' id='" +self.dataset['instance'] + "_variable_1' value='free' checked='checked' />" + 'Free Space [' + self.dataset['unit'] + ']')
        help_appending.append("<input type='checkbox' id='" +self.dataset['instance'] + "_variable_2' value='total-free' checked='checked' />" + 'Used Space [' + self.dataset['unit'] + ']')
        help_appending.append("<input type='checkbox' id='" +self.dataset['instance'] + "_variable_3' value='precious' checked='checked' />" + 'Precious Space [' + self.dataset['unit'] + ']')
        help_appending.append("<input type='checkbox' id='" +self.dataset['instance'] + "_variable_4' value='removable' checked='checked' />" + 'Removable Space [' + self.dataset['unit'] + ']')

        if special_details is not None:
          for i,special in enumerate(special_details):
            helpstring = str(special[1])
            helpstring = helpstring.replace('r', 'removable')
            helpstring = helpstring.replace('t', 'total')
            helpstring = helpstring.replace('f', 'free')
            helpstring = helpstring.replace('p', 'precious')
            help_appending.append(str("<input type='checkbox' id='" +self.dataset['instance'] + "_variable_%i' value="%int(i + 5)) + str(helpstring) + " checked='checked' />" + str(special[0]))

        details_finished_list.append(help_appending)

        help_appending = []
        help_appending.append('none')
        help_appending.append(str("<input id='" +self.dataset['instance'] + "_toggle_button' type='button' value='Toggle Selection' onfocus='this.blur()' onclick=" +self.dataset['instance'] + "_toggle('a')/>"))
        help_appending.append(str("<button onfocus='this.blur()' onclick=" +self.dataset['instance'] + "_col_button('total')>Plot Col</button>"))
        help_appending.append(str("<button onfocus='this.blur()' onclick=" +self.dataset['instance'] + "_col_button('free')>Plot Col</button>"))
        help_appending.append(str("<button onfocus='this.blur()' onclick=" +self.dataset['instance'] + "_col_button('total-free')>Plot Col</button>"))
        help_appending.append(str("<button onfocus='this.blur()' onclick=" +self.dataset['instance'] + "_col_button('precious')>Plot Col</button>"))
        help_appending.append(str("<button onfocus='this.blur()' onclick=" +self.dataset['instance'] + "_col_button('removable')>Plot Col</button>"))

        if special_details is not None:
          for i,special in enumerate(special_details):
            helpstring = special[1]
            helpstring = helpstring.replace('r', 'removable')
            helpstring = helpstring.replace('t', 'total')
            helpstring = helpstring.replace('f', 'free')
            helpstring = helpstring.replace('p', 'precious')
            help_appending.append(str("<button onfocus='this.blur()' onclick=" +self.dataset['instance'] + "_col_button('" + helpstring + "')>Plot Col</button>"))
        details_finished_list.append(help_appending)

        for i,pool in enumerate(details_list):
            help_appending= []
            if pool['status'] == 1.0:
                help_appending.append('ok')
            elif pool['status'] == 0.5:
                help_appending.append('warning')
            else:
                help_appending.append('critical')

            r = pool['removable']
            t = pool['total']
            f = pool['free']
            p = pool['precious']

            help_appending.append(pool['poolname'])
            help_appending.append(str('%0.2f' % t))
            help_appending.append(str('%0.2f' % f))
            help_appending.append(str('%0.2f' % float(t-f)))
            help_appending.append(str('%0.2f' % p))
            help_appending.append(str('%0.2f' % r))
            if special_details is not None:
                for i,special in enumerate(special_details):
                    try:
                        help_appending.append(str('%0.2f' % eval(special[2])))
                    except ValueError, TypeError:
                        help_appending.append('matherror')
                    except TypeError:
                        help_appending.append('typeerror')

            details_finished_list.append(help_appending)

        data['details'] = details_finished_list
        data['overview'] = overview_list

        return data
