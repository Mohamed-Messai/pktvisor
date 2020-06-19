import json
import logging
import os
from hashlib import md5
import pandas as pd

import requests

LOG = logging.getLogger(__name__)

TS_TIME_FMT = '%Y/%m/%d-%H:%M:%S'


class TSBase:
    ''' these classes should interface between a specific TSDB backend and return only pandas data models,
        no bokeh models or knowledge'''

    def __init__(self, url, force=False):
        self.url = url
        self.force = force

    def query(self, query):
        raise NotImplementedError()


class Elastic(TSBase):
    DEFAULT_INDEX = 'pktvisor3'

    # https://www.elastic.co/guide/en/elasticsearch/reference/5.5/search-aggregations-bucket-terms-aggregation.html#_size
    MAX_TERM_SIZE = 1000

    def __init__(self, *args, **kwargs):
        super(Elastic, self).__init__(*args, **kwargs)

    # get a list of values for the given terms
    def get_term_vals(self, metric: str, term_list: list):
        aggs = {}
        for t in term_list:
            if t not in aggs:
                aggs[t] = {}
            aggs[t]['terms'] = {'field': t, 'size': self.MAX_TERM_SIZE}
        o = self.query(metric, aggs)
        result = {}
        for t in term_list:
            tset = set()
            for b in o['aggregations'][t]['buckets']:
                tset.add(b['key'])
            result[t] = tset
        return result

    def _extract_value(self, data_set: dict, value_field: str):
        assert value_field in data_set
        return data_set[value_field]['value']

    def _extract_part(self, data_set: dict, agg_list: list):

        if len(agg_list) == 1:
            return self._extract_value(data_set, agg_list[0])

        assert agg_list[0] in data_set, "%s not in data_set" % agg_list[0]

        result = {}
        for bucket in data_set[agg_list[0]]['buckets']:
            result[bucket['key']] = self._extract_part(bucket, agg_list[1:])

        return result

    def extract_aggs(self, data_set: dict, agg_list: list):

        if 'aggregations' in data_set:
            data_set = data_set['aggregations']

        return self._extract_part(data_set, agg_list)

    def extract_ts_dataframe(self, data_set: dict, agg_list: list, id_map=None, val_map=None):

        # row, column, value
        assert 2 <= len(agg_list) <= 3, "len of agg_list is %s" % len(agg_list)

        identity = lambda x: x
        stringfy = lambda x: str(x)

        if id_map is None:
            id_map = stringfy
        if val_map is None:
            val_map = identity

        # the total numbers of samples we expect per id (also the number of date buckets we expect based on interval)
        # 86400 = secs/day
        num_samples = int((86400 / self.report.downsample) * self.report.num_days)

        # the number of date buckets that we actually got in the data
        num_date_buckets = len(data_set['aggregations']['date']['buckets'])
        if num_date_buckets == 0:
            LOG.warning("NO DATE BUCKETS, SKIPPING TS EXTRACTION")
            return None

        vals = self.extract_aggs(data_set, agg_list)

        # if agg_list is only 1, enforce a single column
        if len(agg_list) == 2:
            vals = {k: {agg_list[1]: v} for k, v in vals.items()}

        assert len(vals.keys()) == num_date_buckets, "mismatch while extracting date bucket values"

        # if samples < date buckets, we are missing data; fill in time buckets with 0s
        if num_samples > num_date_buckets:
            highest_ts = sorted(vals.keys(), reverse=True)[0]
            for i in range(0, num_samples - num_date_buckets):
                highest_ts += self.report.downsample
                vals[highest_ts] = {}
        # if date buckets > samples, we got too much data back from tsdb somehow
        elif num_date_buckets > num_samples:
            LOG.warning('too many date buckets returned, truncating to %s' % num_samples)
            for i in range(0, num_date_buckets - num_samples):
                vals.popitem()

        assert len(vals.keys()) == num_samples, 'mismatched number of date buckets'

        # convert ELK epoch_seconds to normal unix timestamp, then make a DateTime index
        date_index = pd.to_datetime(list(map(lambda x: x / 1000, vals.keys())), unit='s', utc=True)

        # get full set of ids so we can fill missing data later
        id_set = set()
        for ts in vals:
            id_set.update(set(vals[ts].keys()))

        vals_by_id = {}
        for ts in vals:
            # XXX assert ts == date_index[n]
            if len(vals[ts].keys()) != len(id_set):
                LOG.debug('short ts %s id size: %s' % (ts, len(vals[ts].keys())))
            for id in vals[ts]:
                new_id = id_map(id)
                if new_id not in vals_by_id:
                    vals_by_id[new_id] = []
                val = val_map(vals[ts][id])
                vals_by_id[new_id].append(val)
            # make sure each time bucket has the same number of ids. fill in 0s where data is missing by key.
            missing_id_set = id_set - set(vals[ts].keys())
            if len(missing_id_set):
                LOG.debug('ts %s had missing data, will fill ids: %s' % (ts, [id_map(i) for i in missing_id_set]))
                for missing_id in missing_id_set:
                    missing_id = id_map(missing_id)
                    if missing_id not in vals_by_id:
                        vals_by_id[missing_id] = []
                    vals_by_id[missing_id].append(0)

        # create final series and dataframe
        series_list = {}
        for id in vals_by_id:
            assert len(
                vals_by_id[id]) == num_samples, 'id %s does not have enough values (%s) based on date interval (%s)' % (
                id,
                len(vals_by_id[new_id]), num_samples)
            series_list[id] = pd.Series(vals_by_id[id], index=date_index)

        frame = pd.DataFrame(series_list)

        return frame

    def query(self, metric: tuple = None, aggs={}, term_filters: dict = None, script_filters: dict = None,
              index: str = None, term_must_nots: dict = None, start_time='now-1d/d', end_time='now/d'):

        query = {
            'size': 0,
            'query': {
                'bool': {
                    'filter': [
                        {'range': {'@timestamp': {"gte": start_time,
                                                  "lte": end_time,
                                                  "format": "yyyy/MM/dd-HH:mm:ss"}}},
                    ]
                }
            },
            'aggs': aggs
        }

        if metric:
            assert len(metric) == 2
            query['query']['bool']['filter'].append({'term': {metric[0]: metric[1]}})

        if script_filters:
            for k, v in script_filters.items():
                query['query']['bool']['filter'].append({'script': {k: v}})

        if term_filters:
            for k, v in term_filters.items():
                query['query']['bool']['filter'].append({'term': {k: v}})

        if term_must_nots:
            query['query']['bool']['must_not'] = []
            for k, v in term_must_nots.items():
                query['query']['bool']['must_not'].append({'term': {k: v}})

        squery = json.dumps(query)
        LOG.debug(squery)

        hashid = md5(squery.encode('latin1')).hexdigest()
        fname_base = 'report'
        outputdir = 'output/%s/' % fname_base  # type: str
        cachedir = '%scache/' % outputdir  # type: str
        os.makedirs(cachedir, exist_ok=True)

        dfname = '%s%s-%s.json' % (cachedir, fname_base, hashid)
        qfname = '%s%s-%s.query' % (cachedir, fname_base, hashid)

        baseurl = self.url
        if index is None:
            index = self.DEFAULT_INDEX
        url = '%s%s-*/_search' % (baseurl, index)

        def load_from_cache():
            f = open(dfname, 'r')
            LOG.debug("getting TS data from cache %s" % dfname)
            output = f.readline()
            output = json.loads(output)
            f.close()
            return output

        def load_from_server():
            LOG.info("getting TS data from server: %s" % url)
            f = open(qfname, 'w')
            f.write("GET %s-*/_search\n" % index)
            f.write("%s\n" % squery)
            f.close()
            r = requests.post(url, json=query)
            if r.status_code != 200:
                raise Exception(r.json())
            output = r.json()
            f = open(dfname, 'w')
            f.write(json.dumps(output))
            f.close()
            return output

        if self.force:
            output = load_from_server()
        else:
            try:
                output = load_from_cache()
            except IOError:
                output = load_from_server()

        return output
