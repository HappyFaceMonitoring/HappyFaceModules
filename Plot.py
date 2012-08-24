
import hf, time
from sqlalchemy import *

try:
    import imghdr
except ImportError:
    self.logger.warning("imghdr module not found, Plot module will not be able \
to check if downloaded file is actuallly an image.")
    imghdr = None

class Plot(hf.module.ModuleBase):
    config_keys = {
        'plot_url': ('URL of the image to display', ''),
        'use_start_end_time': ('Enable the mechanism to include two timestamps in the GET part of the URL', 'False'),
        'starttime_parameter_name': ('Name of the GET argument for the starting timestamp', 'starttime'),
        'endtime_parameter_name': ('Name of the GET argument for the end timestamp, which is now', 'endtime'),
        'timerange_seconds': ('How far in the past is the start timestamp (in seconds)', '259200'),
    }
    config_hint = ''
    
    table_columns = [
        Column("plot_file", TEXT)
    ], ['plot_file']
    

    def prepareAcquisition(self):
        
        url = self.config["plot_url"]
        use_start_end_time = False
        try:
            use_start_end_time = self.config["use_start_end_time"] == "True"
        except KeyError, e:
            pass
        if use_start_end_time:
            try:
                url += "&"+self.config["starttime_parameter_name"]+"="+str(int(time.time())-int(self.config["timerange_seconds"]))
            except KeyError, e:
                pass
            try:
                url += "&"+self.config["endtime_parameter_name"]+"="+str(int(time.time()))
            except KeyError, e:
                pass
            
        self.plot = hf.downloadService.addDownload(url)
        
    def extractData(self):
        data = {
            "source_url": self.plot.getSourceUrl()
        }
        if self.plot.isDownloaded():
            if imghdr:
                extension = imghdr.what(self.plot.getTmpPath())
            else:
                extension = 'png'
            if extension is not None:
                self.plot.copyToArchive(self, "." + extension)
                data["plot_file"] = self.plot
            else:
                data.update({
                "plot_file": None,
                "status": -1.0,
                "error_string": "Downloaded file was not an image, probably source server failed to deliver file.",
                "source_url": self.plot.getSourceUrl(),
            })
        else:
            data.update({
                "plot_file": None,
                "status": -1.0,
                "error_string": "Plot was not downloaded :"+self.plot.error,
                "source_url": self.plot.getSourceUrl(),
            })
        return data

