# Module for Status of CMS6 via condor_q

import hf
from sqlalchemy import *
import json
import time
import datetime


class CacheHitMiss(hf.module.ModuleBase):
    config_keys = {'sourceurl': ('Source Url', ''),
                   'plotsize_x': ('size of the plot in x', '8.9'),
                   'plotsize_y': ('size of plot in y', '5'),
                   'time_limit': ('in days max 30 days', '7'),
                   'nbins': ('number of bins in histograms', '5')
                   }
    table_columns = [
        Column('filename_plot', TEXT),
        Column('error_msg', TEXT)
    ], ['filename_plot']


    def prepareAcquisition(self):
        link = self.config['sourceurl']
        self.plotsize_x = float(self.config['plotsize_x'])
        self.plotsize_y = float(self.config['plotsize_y'])
        self.nbins = int(self.config['nbins'])
        self.time_limit = int(self.config['time_limit'])
        self.time_limits = time.time() - self.time_limit*24*60*60
        # Download the file
        self.source = hf.downloadService.addDownload(link)
        # Get URL
        self.source_url = self.source.getSourceUrl()
        # Set up Container for subtable data

    def extractData(self):
        import matplotlib.pyplot as plt
        from matplotlib.font_manager import FontProperties
        import numpy as np
        data = {}
        data['filename_plot'] = ""
        data['error_msg'] = ""
        path = self.source.getTmpPath()
        # Function to convert seconds to readable time format
        # open file
        with open(path, 'r') as f:
            # fix the JSON-File, so the file is valid
            content = f.read()
            services = json.loads(content)
        if services['error'] != "":
            data['status'] = 0
            data['error_msg'] = "Connection to Coordinator failed"
            return data
        id_list = services['jobs'].keys()
        hit_list = []
        local_list = []
        for id in id_list:
            if services['jobs'][id]['creation_time']> self.time_limits:
                hit_list.append(float(services['jobs'][id]['cachehit_rate']))
                local_list.append(float(services['jobs'][id]['locality_rate']))
        # generate 2d histogram
        nbins = 1.0/(self.nbins)
        bins = [np.arange(0.0, 1.1, nbins), np.arange(0.0, 1.1, nbins)]
        H, xedges, yedges = np.histogram2d(hit_list, local_list, bins=bins)
        fig = plt.figure(figsize=(self.plotsize_x, self.plotsize_y))
        H = np.rot90(H)
        H = np.flipud(H)
        plt.pcolor(xedges, yedges, H, cmap='Blues')
        cbar = plt.colorbar()
        cbar.ax.set_ylabel('Jobs')
        plt.ylabel('locality rate')
        plt.xlabel('cachehit rate')
        time_readable = datetime.datetime.fromtimestamp(
            float(self.time_limit)).strftime('%Y-%m-%d %H:%M:%S')
        plt.title('Cache Hit Distribution for the last ' + str(self.time_limit) + " days")
        plt.tight_layout()
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + "_filesize.png"), dpi=91)
        data["filename_plot"] = self.instance_name + "_filesize.png"
        print data
        return data
