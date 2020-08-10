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
from bokeh.models import TableColumn, DataTable
from bokeh.models import ColumnDataSource, PreText, Select, Button
from bokeh.plotting import figure
from bokeh.server.server import Server
from lib.tsdb import Elastic

LOG = logging.getLogger(__name__)

DATA_DIR = join(dirname(__file__), 'daily')

DEFAULT_TICKERS = ['AAPL', 'GOOG', 'INTC', 'BRCM', 'YHOO']

TOP_N = ["dns_top_qname2",
         "dns_top_qname3",
         "dns_top_nxdomain",
         "dns_top_qtype",
         "dns_top_rcode",
         "dns_top_refused",
         "dns_top_srvfail",
         "dns_top_udp_ports",
         "dns_xact_in_top_slow",
         "dns_xact_out_top_slow",
         "packets_top_ASN",
         "packets_top_geoLoc",
         "packets_top_ipv4",
         "packets_top_ipv6",
         ]


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


def get_data_table():  # top_n_table: dict):
    stats_table = dict(
        # name=top_n_table.keys(),
        # value=top_n_table.values()
    )

    source = ColumnDataSource(stats_table)

    columns = [
        TableColumn(field="name", title="Name"),
        TableColumn(field="value", title="Value"),
    ]
    return DataTable(source=source, columns=columns, width=400, height=400, sortable=False)


def setup():
    global stats, ticker1, ticker2, ticker3, source, source_static, corr, ts1, ts2, opts, topology, button
    # stats = PreText(text='', width=500)
    topology = get_variables(opts['ELASTIC_URL'])
    print(topology)

    network_list = ['-'] + [*topology]
    # network = network_list[0]
    # pop_list = [*toplogy[network]]
    # pop = pop_list[0]
    # host_list = [*topology[network][pop]]
    # host = host_list[0]
    ticker1 = Select(value='-', options=network_list)
    ticker2 = Select(options=['-'])
    ticker3 = Select(options=['-'])

    button = Button(label="Go", button_type="success")
    button.on_click(go_button)
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


def go_button():
    update()


def ticker1_change(attrname, old, new):
    global ticker2, ticker3, topology
    network = new
    if network == '-':
        return
    pop_list = ['-'] + [*topology[network]]
    pop = '-'
    host_list = ['-']
    host = '-'
    ticker2.options = pop_list
    ticker2.value = pop
    ticker3.options = host_list
    ticker3.value = host


def ticker2_change(attrname, old, new):
    global ticker1, ticker3, topology
    network = ticker1.value
    pop = new
    if pop == '-':
        return
    host_list = ['-'] + [*topology[network][pop]]
    host = '-'
    ticker3.options = host_list
    ticker3.value = host


def ticker3_change(attrname, old, new):
    pass


def update_top_n(top_n):
    global top_tables, TOP_N

    for top in TOP_N:
        top_tables[top].source.data = dict(
            {'name': list(top_n[top].keys()), 'value': list(top_n[top].values())})


def update(selected=None):
    global ticker1, ticker2, ticker3, opts
    network = ticker1.value
    pop = ticker2.value
    host = ticker3.value
    if network == '-':
        return
    top_n = get_top_n(opts['ELASTIC_URL'], network, pop, host)
    update_top_n(top_n)

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
    global stats, ticker1, ticker2, ticker3, source, source_static, corr, ts1, ts2, button, top_tables, TOP_N
    setup()

    top_tables = dict()
    rows = []
    rows.append(row(ticker1, ticker2, ticker3, button))
    top_row = []
    for top in TOP_N:
        top_tables[top] = get_data_table()
        top_row.append(top_tables[top])
        if len(top_row) == 3:
            rows.append(row(children=top_row))
            top_row = []
    layout = column(children=rows)

    # initialize
    update()

    doc.add_root(layout)
    doc.title = "pktvisor"


def get_top_n(url, network, pop, host):
    global TOP_N
    aggs = {"top_n": {
        "scripted_metric": {
            "init_script": """
        state.top_n = new HashMap();
      """,
            "map_script": """
      long deep = doc["http.packets_deep_samples"][0].longValue();
      long total = doc["http.packets_total"][0].longValue();
      double adjust = 1.0;
      if (total > 0L && deep > 0L) {
        adjust = Math.round(1.0 / (deep.doubleValue() / total.doubleValue()));            
      }
      for (Map.Entry entry: state.top_n.entrySet()) {
        for (int i = 0; i <= 9; i++) {
          String name_key = "http." + entry.getKey() + "_" + String.valueOf(i) + "_name.raw";
          String val_key = "http." + entry.getKey() + "_" + String.valueOf(i) + "_estimate";
          if (doc.containsKey(name_key) && doc[name_key].size() > 0 && doc[val_key].size() > 0) {
            String name = doc[name_key][0].toLowerCase();
            long val = doc[val_key][0].longValue();
            if (state.top_n[entry.getKey()].containsKey(name)) {
              state.top_n[entry.getKey()][name] += (long)(val*adjust);              
            }
            else {
              state.top_n[entry.getKey()][name] = (long)(val*adjust);
            }
          }
        }
      }
      """,
            "combine_script": """
      for (Map.Entry entry: state.top_n.entrySet()) {
        ArrayList list = state.top_n[entry.getKey()].entrySet().stream().sorted(Map.Entry.comparingByValue())
        .collect(Collectors.toList());
        Collections.reverse(list);
        state.top_n[entry.getKey()].clear();
        int i = 0;
        for (Map.Entry subentry: list) {
          i++;
          if (i > 10)
            break;
          state.top_n[entry.getKey()].put(subentry.getKey(), subentry.getValue());
        }
      }
      return state.top_n;
      """,
            "reduce_script": """
      HashMap top_n = new HashMap();
      for (shard_map in states) {
        for (Map.Entry entry : shard_map.entrySet()) {
          if (!top_n.containsKey(entry.getKey())) {
            top_n[entry.getKey()] = new LinkedHashMap();              
          }
          for (Map.Entry subentry : entry.getValue().entrySet()) {
            if (top_n[entry.getKey()].containsKey(subentry.getKey())) {
              top_n[entry.getKey()][subentry.getKey()] += subentry.getValue();              
            }
            else {
              top_n[entry.getKey()][subentry.getKey()] = subentry.getValue();  
            }
          }
        }
      }
      for (Map.Entry entry: top_n.entrySet()) {
        ArrayList list = top_n[entry.getKey()].entrySet().stream().sorted(Map.Entry.comparingByValue())
        .collect(Collectors.toList());
        Collections.reverse(list);
        top_n[entry.getKey()].clear();
        int i = 0;
        for (Map.Entry subentry: list) {
          i++;
          if (i > 10)
            break;
          top_n[entry.getKey()].put(subentry.getKey(), subentry.getValue());
        }
      }        
      return top_n;
      """
        }
    }
    }

    for top in TOP_N:
        aggs["top_n"]["scripted_metric"]["init_script"] += "state.top_n[\"" + top + "\"] = new LinkedHashMap();\n"

    term_filters = {
        'network.raw': network,
    }
    if pop != '-':
        term_filters['pop.raw'] = pop
    if host != '-':
        term_filters['host.raw'] = host
    tsdb = Elastic(url)
    result = tsdb.query(None, aggs, term_filters=term_filters, index='pktvisor3')
    print(result)

    return result['aggregations']['top_n']['value']


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
