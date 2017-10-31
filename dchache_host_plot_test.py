
import json
import re
from sqlalchemy import Column, TEXT
import urllib


class PlotdCache(object):

    def prepareAcquisition(self):
        self.source_url = 'http://cmsdcacheweb-kit.gridka.de:2288/context/transfers.json'
        self.config = {'pool': 'D_cms', 'category': 'host', 'host': 'f01-124-127,f01-120-129,'
                                                                    'f01-120-127'}

    def extractData(self):
        hostRE = re.compile(r'f\d{2}-\d{3}-\d{3}')
        data = {}
        self.pool_data = {}
        self.sum_prot = {}
        self.sum_pool = {}
        self.difference = []
        response = urllib.urlopen(self.source_url)
        if self.config['category'] == 'pool':
            objects_of_interest = [element for element in self.config['pool'].split(',')]
        else:
            if self.config['category'] == 'host':
                objects_of_interest = [element for element in self.config['host'].split(',')]
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
                        self.pool_data.setdefault(pool, {}).setdefault(direction, {}).setdefault(prot, []).append(throughput)
                        self.sum_pool[direction][pool] = self.sum_pool.setdefault(direction, {}).get(pool, 0) + throughput
                    else:
                        if self.config['category'] == 'host':
                            if pool == '<unknown>':
                                continue
                            self.pool_data.setdefault(pool, {}).setdefault(direction, {}).setdefault(prot, []).append(throughput)
                            self.sum_pool[direction][pool] = self.sum_pool.setdefault(direction, {}).get(pool, 0) + throughput
                    # Calculate the difference between waiting and start time.
                    self.difference.append(entry.get('moverStart') - entry.get('waitingSince'))
                    self.sum_prot[direction][prot] = self.sum_prot.setdefault(direction, {}).get(prot, 0) + throughput
        if self.config['category'] == 'pool':
            self.plot_objects = sorted(self.pool_data, key=lambda p: (p.split('_')[1][1:], p))
            self.plot_objects.reverse()
        else:
            if self.config['category'] == 'host':
                self.plot_objects = sorted(self.pool_data)
                self.plot_objects.reverse()
        # Plot creation for user statistics
        data["filename_plot"] = self.plot()
        return data

    def plot(self):
        import numpy
        import matplotlib
        matplotlib.use("agg")
        import matplotlib.pyplot
        color = {
            'dcap-3|0': '#ff0000',
            'dcap-3|1': '#bb0000',

            'GFtp-1|0': '#0000ff',
            'GFtp-1|1': '#0000bb',

            'GFtp-2|0': '#6666ff',
            'GFtp-2|1': '#6666bb',

            'Xrootd-2.7|0': '#00ffff',
            'Xrootd-2.7|1': '#00bbbb',

            'NFSV4.1-0|0': '#7CFC00',
            'NFSV4.1-0|1': '#ADFF2F',
        }
        # Create Plot.
        plot_height = len(self.plot_objects) * 10. / 12
        fig = matplotlib.pyplot.figure(figsize=(7, plot_height))
        fig.subplots_adjust(left=0.03, right=0.97, top=0.99, bottom=0.075, wspace=0.95)

        ax = {}
        ax['in'] = fig.add_subplot(121)
        ax['out'] = fig.add_subplot(122)
        ax['in'].yaxis.set_ticks([])
        ax['out'].yaxis.set_ticks(range(len(self.plot_objects)))
        ax['out'].yaxis.set_ticklabels(self.plot_objects)
        ax['in'].set_xlabel('Incoming [MB/s]')
        ax['out'].set_xlabel('Outgoing [MB/s]')
        ax['in'].set_xlim((max(self.sum_pool.get('in', {None: 0}).values()) / 1.e3 + 10, 0))
        ax['out'].set_xlim((0, max(self.sum_pool.get('out', {None: 0}).values()) / 1.e3 + 10))

        for direction in ['in', 'out']:
            for pidx, pool in enumerate(self.plot_objects):
                position = 0
                pool_info = self.pool_data.get(pool, {}).get(direction, {})
                for prot in sorted(pool_info):
                    for sidx, entry in enumerate(pool_info[prot]):
                        entry = entry / 1000.
                        ax[direction].barh(pidx - 0.5, entry, 1, position,
                                           color=color['%s|%d' % (prot, sidx % 2)], linewidth=0)
                        position += entry

            if direction == 'in':
                patches = []
                labels = []
                for prot in sorted(set(map(lambda s: s.split('|')[0], color))):
                    item = ''
                    if self.sum_prot.get('in', {}).get(prot, 0) or self.sum_prot.get('out', {}).get(prot, 0):
                        item = prot
                    if self.sum_prot.get('out', {}).get(prot, 0):
                        item += '\nread: %.1f MB/s' % (self.sum_prot['out'][prot] / 1e3)
                    if self.sum_prot.get('in', {}).get(prot, 0):
                        item += '\nwrite: %.1f MB/s' % (self.sum_prot['in'][prot] / 1e3)
                    if item:
                        patches.append(matplotlib.patches.Rectangle((0, 0), 1, 1, color=color['%s|0' % prot]))
                        labels.append(item)
                ax[direction].set_xticklabels(map(lambda x: '%d' % x, ax[direction].xaxis.get_ticklocs()), rotation=45)
            else:
                ax[direction].set_xticklabels(map(lambda x: '%d' % x, ax[direction].xaxis.get_ticklocs()), rotation=-45)
            ax[direction].set_ylim((-1, len(self.plot_objects) - 1))
            ax[direction].grid(axis='x')

        ax['in'].legend(patches, labels, labelspacing=1, prop=matplotlib.font_manager.FontProperties(size=9),
                        loc='best')

        fig_diff = matplotlib.pyplot.figure(figsize=(7, 7))
        ax_diff = fig_diff.add_subplot(111)
        n, bins, _ = ax_diff.hist(self.difference, max(self.difference)+1, range=(-0.5, max(self.difference) + 0.5),
                                  color="slateblue", fill=True, histtype='bar', align='mid')
        ax_diff.set_xlabel('difference in s')
        ax_diff.set_ylabel('number of transfers')
        ax_diff.set_title('Difference between waiting and starting time of transfers')
        ax_diff.set_xlim(min(bins), max(bins))

        # save figure
        plotname = './testbild.png'
        fig.savefig(plotname, dpi=91, bbox_inches="tight")
        fig_diff.savefig(plotname.replace(".png", "_timedifferences.png"), dpi=91, bbox_inches="tight")
        return plotname

myplot = PlotdCache()
myplot.prepareAcquisition()
data = myplot.extractData()
