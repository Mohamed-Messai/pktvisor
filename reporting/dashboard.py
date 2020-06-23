"""
Dashboard for pktvisor

 Usage:
   dashboard.py ELASTIC_URL [-v VERBOSITY]
   dashboard.py (-h | --help)

 Options:
   -h --help        Show this screen.
   -v VERBOSITY     How verbose output should be, 0 is silent [default: 1]

"""

from functools import lru_cache
from os.path import dirname, join
import logging
import docopt

import pandas as pd

from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, PreText, Select
from bokeh.plotting import figure
from bokeh.server.server import Server
from lib.tsdb import Elastic

LOG = logging.getLogger(__name__)

DATA_DIR = join(dirname(__file__), 'daily')

DEFAULT_TICKERS = ['AAPL', 'GOOG', 'INTC', 'BRCM', 'YHOO']


def nix(val, lst):
    return [x for x in lst if x != val]


@lru_cache()
def load_ticker(ticker):
    fname = join(DATA_DIR, 'table_%s.csv' % ticker.lower())
    data = pd.read_csv(fname, header=None, parse_dates=['date'],
                       names=['date', 'foo', 'o', 'h', 'l', 'c', 'v'])
    data = data.set_index('date')
    return pd.DataFrame({ticker: data.c, ticker + '_returns': data.c.diff()})


@lru_cache()
def get_data(t1, t2):
    df1 = load_ticker(t1)
    df2 = load_ticker(t2)
    data = pd.concat([df1, df2], axis=1)
    data = data.dropna()
    data['t1'] = data[t1]
    data['t2'] = data[t2]
    data['t1_returns'] = data[t1 + '_returns']
    data['t2_returns'] = data[t2 + '_returns']
    return data


def setup():
    global stats, ticker1, ticker2, ticker3, source, source_static, corr, ts1, ts2, opts, topology
    stats = PreText(text='', width=500)
    topology = get_variables(opts['ELASTIC_URL'])

    network_list = [*topology]
    network = network_list[0]
    pop_list = [*topology[network]]
    pop = pop_list[0]
    host_list = [*topology[network][pop]]
    host = host_list[0]
    ticker1 = Select(value=network, options=network_list)
    ticker2 = Select(value=pop, options=pop_list)
    ticker3 = Select(value=host, options=host_list)

    # source = ColumnDataSource(data=dict(date=[], t1=[], t2=[], t1_returns=[], t2_returns=[]))
    # source_static = ColumnDataSource(data=dict(date=[], t1=[], t2=[], t1_returns=[], t2_returns=[]))
    # tools = 'pan,wheel_zoom,xbox_select,reset'
    #
    # corr = figure(plot_width=350, plot_height=350,
    #               tools='pan,wheel_zoom,box_select,reset')
    # corr.circle('t1_returns', 't2_returns', size=2, source=source,
    #             selection_color="orange", alpha=0.6, nonselection_alpha=0.1, selection_alpha=0.4)
    #
    # ts1 = figure(plot_width=900, plot_height=200, tools=tools, x_axis_type='datetime', active_drag="xbox_select")
    # ts1.line('date', 't1', source=source_static)
    # ts1.circle('date', 't1', size=1, source=source, color=None, selection_color="orange")
    #
    # ts2 = figure(plot_width=900, plot_height=200, tools=tools, x_axis_type='datetime', active_drag="xbox_select")
    # ts2.x_range = ts1.x_range
    # ts2.line('date', 't2', source=source_static)
    # ts2.circle('date', 't2', size=1, source=source, color=None, selection_color="orange")
    ticker1.on_change('value', ticker1_change)
    ticker2.on_change('value', ticker2_change)
    ticker3.on_change('value', ticker3_change)
    # source.selected.on_change('indices', selection_change)


def ticker1_change(attrname, old, new):
    global ticker2, ticker3, topology
    network = new
    pop_list = [*topology[network]]
    pop = pop_list[0]
    host_list = [*topology[network][pop]]
    host = host_list[0]
    ticker2.options = pop_list
    ticker2.value = pop
    ticker3.options = host_list
    ticker3.value = host
    update()


def ticker2_change(attrname, old, new):
    global ticker1, ticker3, topology
    network = ticker1.value
    pop = new
    host_list = [*topology[network][pop]]
    host = host_list[0]
    ticker3.options = host_list
    ticker3.value = host
    update()

def ticker3_change(attrname, old, new):
    pass

def update(selected=None):
    global ticker1, ticker2, ticker3
    network = ticker1.value
    pop = ticker2.value
    host = ticker3.value

    # global source, source_static, corr, ts1, ts2
    # t1, t2 = ticker1.value, ticker2.value

    # df = get_data(t1, t2)
    # data = df[['t1', 't2', 't1_returns', 't2_returns']]
    # source.data = data
    # source_static.data = data
    #
    # update_stats(df, t1, t2)
    #
    # corr.title.text = '%s returns vs. %s returns' % (t1, t2)
    # ts1.title.text, ts2.title.text = t1, t2


def update_stats(data, t1, t2):
    stats.text = str(data[[t1, t2, t1 + '_returns', t2 + '_returns']].describe())


def selection_change(attrname, old, new):
    global stats, ticker1, ticker2, ticker3, source, source_static, corr, ts1, ts2
    # t1, t2 = ticker1.value, ticker2.value
    # data = get_data(t1, t2)
    # selected = source.selected.indices
    # if selected:
    #     data = data.iloc[selected, :]
    # update_stats(data, t1, t2)


def app(doc):
    # set up layout
    global stats, ticker1, ticker2, ticker3, source, source_static, corr, ts1, ts2
    setup()
    widgets = column(ticker1, ticker2, ticker3, stats)
    main_row = row(widgets)
    series = column()
    # main_row = row(corr, widgets)
    # series = column(ts1, ts2)
    layout = column(main_row, series)

    # initialize
    update()

    doc.add_root(layout)
    doc.title = "pktvisor"


def get_variables(url):
    aggs = {"networks": {
        "terms": {"field": "network.raw", "size": 200},
        "aggs": {
            "pops": {
                "terms": {"field": "pop.raw", "size": 100},
                "aggs": {
                    "hosts": {
                        "terms": {"field": "host.raw", "size": 50},
                    }
                }
            }
        }
    }
    }

    term_filters = None
    tsdb = Elastic(url)
    result = tsdb.query(None, aggs, term_filters=term_filters)

    topology = {}

    for n in result['aggregations']['networks']['buckets']:
        netid = n['key']
        topology[netid] = {}
        for p in n['pops']['buckets']:
            popid = p['key']
            topology[netid][popid] = {}
            for h in p['hosts']['buckets']:
                hostid = h['key']
                topology[netid][popid][hostid] = {}

    return topology


def main():
    global opts
    opts = docopt.docopt(__doc__, version='1.0')

    if int(opts['-v']) > 1:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    print('Opening Bokeh application on http://localhost:5006/, ELK is at ' + opts['ELASTIC_URL'])

    # Setting num_procs here means we can't touch the IOLoop before now, we must
    # let Server handle that. If you need to explicitly handle IOLoops then you
    # will need to use the lower level BaseServer class.
    server = Server({'/': app}, num_procs=1)
    server.start()

    server.io_loop.start()


if __name__ == "__main__":
    main()
