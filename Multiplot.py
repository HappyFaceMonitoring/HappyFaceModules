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

import hf, time
from sqlalchemy import *

try:
    import imghdr
except ImportError:
    self.logger.warning("imghdr module not found, Plot module will not be able \
to check if downloaded file is actuallly an image.")
    imghdr = None

class Multiplot(hf.module.ModuleBase):
    config_keys = {
        'plot_url01': ('URL of the image to display, for more images use plot_url02 and so on', ''),
        'use_start_end_time': ('Enable the mechanism to include two timestamps in the GET part of the URL', 'False'),
        'starttime_parameter_name': ('Name of the GET argument for the starting timestamp', 'starttime'),
        'endtime_parameter_name': ('Name of the GET argument for the end timestamp, which is now', 'endtime'),
        'timerange_seconds': ('How far in the past is the start timestamp (in seconds)', '259200'),
    }
    config_hint = ''
    
    table_columns = [] , []
    
    subtable_columns = {
        "plots": ([
        Column('plot_file', TEXT)],['plot_file'])
    }
    

    def prepareAcquisition(self):
        
        url = []
        self.plot = []
        for i,group in self.config.iteritems():
	  if 'plot_url' in i:
	    url.append(group)

        use_start_end_time = False
        try:
            use_start_end_time = self.config["use_start_end_time"] == "True"
        except hf.ConfigError:
            pass
        if use_start_end_time:
            for i, group in enumerate(url):
                try:
                    group = group + "&"+self.config["starttime_parameter_name"]+"="+str(int(time.time())-int(self.config["timerange_seconds"]))
                except hf.ConfigError:
                    pass
                try:
                    group = group + "&"+self.config["endtime_parameter_name"]+"="+str(int(time.time()))
                except hf.ConfigError:
                    pass
                appender = {'plot_file' : hf.downloadService.addDownload(group)}
                self.plot.append(appender)
        else:
            for i, group in enumerate(url):
                appender = {'plot_file' : hf.downloadService.addDownload(group)}
                self.plot.append(appender)
                
    def extractData(self):
        data = {}
        data['description'] = ''
        for i,group in enumerate(self.plot):
          data['description'] += 'plot%20i: '%i + group['plot_file'].getSourceUrl()
           
        for i,group in enumerate(self.plot):
            if group['plot_file'].isDownloaded():
                if imghdr:
                    extension = imghdr.what(group['plot_file'].getTmpPath())
                else:
                    extension = 'png'
                if extension is not None:
                    group['plot_file'].copyToArchive(self, str(i) + "." + extension)
        for i,group in enumerate(self.plot):
            group['plot_file'] = group['plot_file'].getArchivePath()
        return data
        
    def fillSubtables(self, parent_id):
        self.subtables['plots'].insert().execute([dict(parent_id=parent_id, **row) for row in self.plot])
   
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        files = self.subtables['plots'].select()\
            .where(self.subtables['plots'].c.parent_id==self.dataset['id'])\
            .execute().fetchall()
        data['files'] = files
        return data
        
