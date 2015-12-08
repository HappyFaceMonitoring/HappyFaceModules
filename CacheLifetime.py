# -*- coding: utf-8 -*-
#
# Copyright 2015 Institut für Experimentelle Kernphysik - Karlsruher Institut für Technologie
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
import json
import time


class CacheLifetime(hf.module.ModuleBase):
    config_keys = {'sourceurl': ('Source Url', ''),
                   'plotsize_x': ('size of the plot in x', '10'),
                   'plotsize_y': ('size of plot in y', '5'),
                   'time_limit': ('in days max 30 days', '7'),
                   'nbins': ('number of bins in histograms', '200')
                   }
    table_columns = [
        Column('filename_plot', TEXT),
        Column('error_msg', TEXT)
    ], ['filename_plot']

    def prepareAcquisition(self):
        link = self.config['sourceurl']
        self.plotsize_x = float(self.config['plotsize_x'])
        self.plotsize_y = float(self.config['plotsize_y'])
        self.nbins = float(self.config['nbins'])
        self.time_limit = int(self.config['time_limit'])
        # Download the file
        self.source = hf.downloadService.addDownload(link)
        # Get URL
        self.source_url = self.source.getSourceUrl()

    def extractData(self):
        import matplotlib.pyplot as plt
        from matplotlib.font_manager import FontProperties
        data = {}
        data['filename_plot'] = ""
        data['error_msg'] = ""
        path = self.source.getTmpPath()
        # open file
        with open(path, 'r') as f:
            # fix the JSON-File, so the file is valid
            content = f.read()
            services = json.loads(content)
        if services['error'] != "":
            data['status'] = 0
            data['error_msg'] = "Connection to Coordinator failed"
            return data
        jobs_id = services['life_time'].keys()
        time_list = list(int(services['life_time'][id]['time']) for id in jobs_id)
        lifetime_list = list(int(services['life_time'][id]['life_time']) for id in jobs_id)
        lifetime_list = map(lambda x: x/(60*60), lifetime_list)
        # TODO if data gets newer, constrain dataset von data from last 7 days etc.
        plot_lifetime_list = []
        for i in xrange(len(time_list)):
            if time_list[i] > time.time() - (self.time_limit*60*60*24):
                plot_lifetime_list.append(lifetime_list[i])
        if len(plot_lifetime_list) == 0:
            data['status'] = 0.5
            data['error_msg'] = "No files removed in the last " + str(self.time_limit) + " days."
            print data
            return data
        fig = plt.figure(figsize=(self.plotsize_x, self.plotsize_y))
        axis = fig.add_subplot(111)
        nbins = self.nbins
        fontLeg = FontProperties()
        fontLeg.set_size('small')
        axis.hist(plot_lifetime_list, nbins, histtype='bar', log=True)
        axis.set_xlabel('Lifetime in hours')
        axis.set_ylabel('Number of Files')
        axis.set_title('Lifetime of Files in Cache')
        plt.tight_layout()
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + ".png"), dpi=91)
        data["filename_plot"] = self.instance_name + ".png"
        print data
        return data
