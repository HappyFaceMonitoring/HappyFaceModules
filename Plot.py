
import hf, time
from sqlalchemy import *

try:
    import imghdr
except ImportError:
    self.logger.warning("imghdr module not found, Plot module will not be able \
to check if downloaded file is actuallly an image.")
    imghdr = None

class Plot(hf.module.ModuleBase):
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
                self.plot.copyToArchive(self.instance_name + "." + extension)
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

plot_table = hf.module.generateModuleTable(Plot, "plot",
[
    Column("plot_file", TEXT)
])

hf.module.addColumnFileReference(plot_table, "plot_file")
hf.module.addModuleClass(Plot)