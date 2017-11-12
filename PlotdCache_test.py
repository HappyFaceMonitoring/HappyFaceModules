import re
import urllib
import json

class PlotdCache(object):
    config_keys = {
        "source_url": ("Not used but filled to avoid warnings.", ""),
        "pool": ("The pool the information is plotted for.", ""),
        "hosts": ("the hosts that the information is plottet for", ""),
        "category": ("The switch that is used to select either grouping by host or pool", "")
        #		"file_loc" : ("Location of the json file used to generate the plot.", "")
    }


    def prepareAcquisition(self):

        # Setting the source url.
        self.config = {'category': 'pool', 'pool': 'D_cms,sT_cms,wT_cms,S_cms,e_ops',
                       'source_url': 'http://cmsdcacheweb-kit.gridka.de:2288/context/transfers.json'}
        self.source_url = self.config["source_url"]

    def extractData(self):
        # define the regular expression, that identifies the Host inside the pool name
        hostRE = re.compile(r'f\d{2}-\d{3}-\d{3}')
        # set up the fields used
        data = {}
        self.pool_data = {}
        self.sum_prot = {}
        self.sum_pool = {}
        self.difference = []
        # get the data from the url
        response = urllib.urlopen(self.source_url)
        # decide if the data being gathered per host or per pool
        if self.config['category'] == 'host':
            objects_of_interest = [element for element in self.config['host'].split(',')]
        else:
            objects_of_interest = [element for element in self.config['pool'].split(',')]
        # go through the dataset and extract the relevant information
        for entry in sorted(json.loads(response.read()), key=lambda x: x.get('moverStart')):
            for element in objects_of_interest:
                if (element in entry.get('pool')) or (element in hostRE.search(entry.get('pool')).group()):
                    if self.config['category'] == 'pool':
                        pool = entry.get('pool')
                    else:
                        if self.config['category'] == 'host':
                            pool = hostRE.search(entry.get('pool')).group()
                    if 'receiving' in entry.get('sessionStatus').lower():
                        direction = 'in'
                    else:
                        direction = 'out'
                    prot = entry.get('protocol')
                    throughput = entry.get('transferRate')
                    if self.config['category'] == 'pool':
                        if pool == '<unknown>':
                            continue
                        self.pool_data.setdefault(pool, {}).setdefault(direction, {}).setdefault(prot, []).append(
                            throughput)
                        self.sum_pool[direction][pool] = self.sum_pool.setdefault(direction, {}).get(pool,
                                                                                                     0) + throughput
                    else:
                        if self.config['category'] == 'host':
                            if pool == '<unknown>':
                                continue
                            self.pool_data.setdefault(pool, {}).setdefault(direction, {}).setdefault(prot, []).append(
                                throughput)
                            self.sum_pool[direction][pool] = self.sum_pool.setdefault(direction, {}).get(pool,
                                                                                                         0) + throughput
                    # Calculate the difference between waiting and start time.
                    # this data is only used to show up in the waiting time statistic, we will have to add in a check so that it wuns nicely
                    if entry.get('moverStart') is not None:
                        self.difference.append(entry.get('moverStart') - entry.get('waitingSince'))
                    # here we add the throughput of the transfer to the throughput of the protocoll
                    self.sum_prot[direction][prot] = self.sum_prot.setdefault(direction, {}).get(prot, 0) + throughput
        # format the data so it can be plotted nicely.
        if self.config['category'] == 'pool':
            self.plot_objects = sorted(self.pool_data, key=lambda p: (p.split('_')[1][1:], p))
            self.plot_objects.reverse()
        else:
            if self.config['category'] == 'host':
                self.plot_objects = sorted(self.pool_data)
                self.plot_objects.reverse()

myobj = PlotdCache()
myobj.prepareAcquisition()
myobj.extractData()
