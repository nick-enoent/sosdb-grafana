from django.db import models
import datetime as dt
import os, sys, traceback
from sosdb import Sos, bollinger
from sosgui import settings, logging
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
import views
import time
import numpy as np

log = logging.MsgLog("Grafana SOS")

event_name_map = { 
    "PAPI_TOT_INS" : "tot_ins",
    "PAPI_TOT_CYC" : "tot_cyc",
    "PAPI_LD_INS"  : "ld_ins",
    "PAPI_SR_INS"  : "sr_ins",
    "PAPI_BR_INS"  : "br_ins",
    "PAPI_FP_OPS"  : "fp_ops",
    "PAPI_L1_ICM"  : "l1_icm",
    "PAPI_L1_DCM"  : "l1_dcm",
    "PAPI_L2_ICA"  : "l2_ica",
    "PAPI_L2_TCA"  : "l2_tca",
    "PAPI_L2_TCM"  : "l2_tcm",
    "PAPI_L3_TCA"  : "l3_tca",
    "PAPI_L3_TCM"  : "l3_tcm"
}

papi_derived_metrics = {
    "cpi" : "cpi",
    "uopi" : "uopi",
    "l1_miss_rate" : "l1_miss_rate",
    "l1_miss_ratio" : "l1_miss_ratio",
    "l2_miss_rate" : "l2_miss_rate",
    "l2_miss_ratio" : "l2_miss_ratio",
    "l3_miss_rate" : "l3_miss_rate",
    "l3_miss_ratio" : "l3_miss_ratio",
    "l2_bw" : "l2_bw",
    "l3_bw" : "l3_bw",
    "fp_rate" : "fp_rate",
    "branch_rate" : "branch_rate",
    "load_rate" : "load_rate",
    "store_rate" : "store_rate"
}

class Search(object):
    def __init__(self, cont):
        self.cont = cont

    def getSchema(self, cont):
        self.schemas = {}
        schemas = {}
        for schema in self.cont.schema_iter():
            name = schema.name()
            schemas[name] = name
        return schemas

    def getIndices(self, cont, schema_name):
        schema = cont.schema_by_name(schema_name)
        indices = {}
        for attr in schema:
            if attr.is_indexed() == True:
                name = attr.name()
                indices[name] = name
        return indices

    def getMetrics(self, cont, schema_name):
        schema = cont.schema_by_name(schema_name)
        attrs = {}
        for attr in schema:
            if attr.type() != Sos.TYPE_JOIN:
                name = attr.name()
                attrs[name] = name
        if schema_name == 'papi-events':
            attrs.update(papi_derived_metrics)
        return attrs

    def getComponents(self, cont, schema_name, start, end):
        schema = cont.schema_by_name(schema_name)
        attr = schema.attr_by_name("component_id")
        if attr is None:
            return {}
        src = SosDataSource()
        src.config(cont=cont)
        where = []
        if start > 0:
            where.append([ 'timestamp', Sos.COND_GE, start ])
        if end > 0:
            where.append([ 'timestamp', Sos.COND_LE, end ])
        src.select([ 'component_id' ],
                   from_    = [ schema_name ],
                   where    = where,
                   order_by = 'timestamp'
        )
        comps = src.get_results()
        if comps is None:
            return {}
        comp_ids = np.unique(comps['component_id'])
        result = {}
        for comp_id in comp_ids:
            result[str(int(comp_id))] = int(comp_id)
        return result

    def getJobs(self, cont, schema_name, start, end):
        schema = cont.schema_by_name(schema_name)
        attr = schema.attr_by_name("job_id")
        if attr is None:
            return {}
        src = SosDataSource()
        src.config(cont=cont)
        where = []
        if start > 0:
            where.append([ 'timestamp', Sos.COND_GE, start ])
        if end > 0:
            where.append([ 'timestamp', Sos.COND_LE, end ])
        src.select([ 'job_id' ],
                   from_    = [ schema_name ],
                   where    = where,
                   order_by = 'timestamp'
        )
        jobs = src.get_results()
        if jobs is None:
            return {}
        job_ids = np.unique(jobs['job_id'])
        result = {}
        for job_id in job_ids:
            if job_id != 0:
                result[str(int(job_id))] = int(job_id)
        return result

class Query(object):
    def __init__(self, cont):
        self.cont = cont
        self.maxDataPoints = 4096

    def getJobComponents(self, job_id):
        """Get components for a particular job"""
        src = SosDataSource()
        src.config(cont=self.cont)
        src.select([ 'component_id' ],
                   from_ = [ 'jobinfo' ],
                   where = [ [ 'job_id', Sos.COND_EQ, job_id ] ],
                   order_by = 'job_time_comp'
                   )
        res = src.get_results(limit=4096)
        if res:
            ucomps = np.unique(res['component_id'])
            return ucomps
        return None

    def getJobCompEnd(self, job_id):
        """Get job end"""
        src = SosDataSource()
        src.config(cont=self.cont)
        src.select([ 'jobinfo.*' ],
                  from_    = [ 'jobinfo' ],
                  where    = [ [ 'job_id', Sos.COND_EQ, job_id ],
                               [ 'job_status', Sos.COND_EQ, 2 ]
                             ],
                  order_by = 'job_comp_time')
        res = src.get_results()
        if res is None:
            return None
        xfrm = Transform(src, None, limit=4096)
        res = xfrm.begin()
        xfrm.max([ 'job_end' ], group_name='component_id')
        comp_time = xfrm.pop()
        nodes = np.arange(comp_time.get_series_size())
        comp_time.append_array(comp_time.get_series_size(), 'node_id', nodes)
        return comp_time

    def getComponents(self, schema, timestamp, start, end):
        """Return unique components with data for this schema"""
        src = SosDataSource()
        src.config(cont=self.cont)
        src.select([ 'component_id' ],
                   from_ = [ schema ],
                   where = [
                       [ timestamp, Sos.COND_GE, start ],
                       [ timestamp, Sos.COND_LE, end ]
                   ],
                   order_by = timestamp
               )
        res = src.get_results(limit=4096)
        if res:
            ucomps = np.unique(res['component_id'])
            return ucomps
        return None

    def getJobTimeseries(self, schemaName,  jobId, metricNames,
                         timestamp, start, end,dataPoints):
        """Return time series data for a particular job"""
        src = SosDataSource()
        src.config(cont=self.cont)

        components = self.getJobComponents(jobId)
        if components is None:
            return []
        result = []
        for comp_id in components:
            src.select(metricNames + [ timestamp ],
                       from_ = [ schemaName ],
                       where = [
                           [ 'component_id', Sos.COND_EQ, comp_id ],
                           [ timestamp, Sos.COND_GE, start ],
                           [ timestamp, Sos.COND_LE, end ],
                       ],
                       order_by = 'comp_time'
                   )
            res = src.get_results()
            if res is None:
                continue
            res['timestamp'] *= 1000
            l = res.series_size
            result.append({ "comp_id" : comp_id, "datapoints" :
                            res.sets[0].array[:l].tolist() })
        return result

    def getPapiTimeseries(self, schemaName, metricNames, job_id,
                          start, end, intervalMs, maxDataPoints):
        """Return time series data for papi-events schema"""
        self.maxDataPoints = maxDataPoints
        src = SosDataSource()
        src.config(cont=self.cont)
        result = []
        if schemaName == 'kokkos_app':
            for metric in metricNames:
                src.select([metric, 'start_time'],
                    from_ = [ schemaName ],
                    where = [
                        [ 'start_time', Sos.COND_GE, start ]
                    ],
                    order_by = 'job_id'
                )
                res = src.get_results()
                l = res.series_size
                result.append({"target" : metric, "datapoints" : res.tolist() })
            return result
        try:
            if not job_id:
                return [{ "target" : "Job Id required for papi_timeseries", datapoints : [] } ]
            xfrm, job = self.getPapiDerivedMetrics(job_id, time_series=True, start=start, end=end)
            for metric in metricNames:
                if metric in event_name_map:
                    metric = event_name_map[metric]
                datapoints = []
                i = 0
                while i < len(job.array(metric)):
                    if i > 0:
                        if job.array('rank')[i-1] != job.array('rank')[i]:
                            result.append({"target" : '['+str(job.array('rank')[i-1])+']'+metric,
                                           "datapoints" : datapoints })
                            datapoints = []
                    dp = [ job.array(metric)[i], job.array('timestamp')[i]/1000 ]
                    datapoints.append(dp)
                    i += 1
                result.append({"target" : '['+str(job.array('rank')[i-1])+']'+metric,
                               "datapoints" : datapoints })
            return result
        except Exception as e:
            a, b, c = sys.exc_info()
            log.write('papi_timeseries '+str(e)+' '+str(c.tb_lineno))
            return None

    def getCompTimeseries(self, schemaName, compIds, metricNames,
                         timestamp, start, end, intervalMs, maxDataPoints):
        """Return time series data for a particular component/s"""
        src = SosDataSource()
        src.config(cont=self.cont)

        result = []
        if compIds:
            if type(compIds) != list:
                compIds = [ int(compIds) ]
        else:
            src.select([ 'component_id', 'timestamp' ],
                       from_ = [ schemaName ],
                       where = [
                           [ 'timestamp', Sos.COND_GE, start ],
                           [ 'timestamp', Sos.COND_LE, end ],
                       ],
                       order_by = 'timestamp'
                   )
            comps = src.get_results(limit=3600)
            if not comps:
                compIds = np.zeros(1)
            else:
                compIds = np.unique(comps['component_id'])
        for comp_id in compIds:
            for metric in metricNames:
                src.select([ metric, 'timestamp' ],
                           from_ = [ schemaName ],
                           where = [
                               [ 'component_id', Sos.COND_EQ, comp_id ],
                               [ timestamp, Sos.COND_GE, start ],
                               [ timestamp, Sos.COND_LE, end ],
                           ],
                           order_by = 'comp_time'
                       )
                inp = None
                if intervalMs < 1000:
                    res = src.get_results(inputer=inp, limit=maxDataPoints)
                else:
                    res = src.get_results(inputer=inp, limit=maxDataPoints, interval_ms=intervalMs)
                    while len(res.array(metric)) < maxDataPoints:
                        rs = src.get_results(inputer=inp, limit=maxDataPoints, interval_ms=intervalMs, reset=False)
                        if not len(rs.array(metric)):
                            break
                        res = res.concat(rs)
                if res is None:
                    return None
                result.append({ "comp_id" : comp_id, "metric" : metric, "datapoints" :
                                res.tolist() })
        return result

    def getJobTable(self, jobId, start, end):
        """Return a table of jobs run in the specified time interval"""
        src = SosDataSource()
        src.config(cont=self.cont)
        where = [
            [ 'timestamp', Sos.COND_GE, start ],
            [ 'timestamp', Sos.COND_LE, end ],
            [ 'job_status', Sos.COND_EQ, 2 ],
            [ 'job_start', Sos.COND_GE, 1 ]
        ]
        if jobId != 0:
            where.insert(0, [ 'job_id', Sos.COND_EQ, jobId ])

        src.select([ '*' ],
                   from_ = [ 'jobinfo' ],
                   where = where,
                   order_by = 'timestamp'
        )
        x = Transform(src, None, limit=12384)
        res = x.begin()
        res = src.get_results()
        if not res:
            return res

        result = x.dup()
        x.min([ 'job_start' ], group_name='job_id',
              keep=[ 'job_id', 'app_id', 'job_name', 'job_user', 'job_status' ],
              xfrm_suffix='')
        result.concat(x.pop())
        x.max([ 'job_end' ], group_name='job_id', xfrm_suffix='')
        result.concat(x.pop())
        nda = result.array('job_start')
        nda *= 1000
        nda1 = result.array('job_end')
        nda1 *= 1000
        return result

    def getTable(self, schemaName, index, metricNames, start, end):
        src = SosDataSource()
        src.config(cont=self.cont)
        if schemaName == 'kokkos_app':
            src.select(metricNames,
                       from_ = [ schemaName ],
                       where = [
                           [ 'start_time', Sos.COND_GE, start ],
                       ],
                       order_by = 'job_id'
            )
        else:
            src.select(metricNames,
                       from_ = [ schemaName ],
                       where = [
                           [ 'timestamp', Sos.COND_GE, start ],
                           [ 'timestamp', Sos.COND_LE, end ]
                       ],
                       order_by = index
            )
        res = src.get_results()
        return res

    def getPapiSumTable(self, schemaName, job_id):
        """Return statistical papi data in table format"""
        try:
            result = {}
            columns = [
                { "text" : "metric" },
                { "text" : "min" },
                { "text" : "max" },
                { "text" : "mean" },
                { "text" : "std_dev" }
            ]
            result['columns'] = columns
            if not job_id:
                print('no job_id')
                return None
            xfrm, job = self.getPapiDerivedMetrics(job_id)
            events, mins, maxs, stats = self.getPapiRankStats(xfrm, job)
            rows = []
            for name in events:
                row = []
                row.append(name)
                row.append(np.nan_to_num(mins.array(name+'_min')[0]))
                row.append(np.nan_to_num(maxs.array(name+'_max')[0]))
                row.append(np.nan_to_num(stats.array(name+'_mean')[0]))
                row.append(np.nan_to_num(stats.array(name+'_std')[0]))
                rows.append(row)
            result["rows"] = rows
            result["type"] = "table"
            res = [ result ]
            return res
        except Exception as e:
            a, b, c = sys.exc_info()
            log.write('getPapiSumTable Err: '+str(e)+str(c.tb_lineno))
            return None

    def papiGetLikeJobs(self, schemaName, job_id, start, end):
        """Return jobs similar to requested job_id based on similar instance data"""
        try:
            src = SosDataSource()
            src.config(cont=self.cont)
            src.select(['inst_data'],
                        from_ = [ 'kokkos_app'],
                        where = [
                            [ 'job_id', Sos.COND_EQ, job_id ],
                        ],
                        order_by = 'job_id',
            )
            res = src.get_results()
            if res is None:
                return None
            result = {}
            jobData = SosDataSource()
            jobData.config(cont=self.cont)
            where = [
                [ 'inst_data' , Sos.COND_EQ, res.array('inst_data')[0] ],
                [ 'start_time', Sos.COND_GE, start ],
                [ 'start_time', Sos.COND_LE, end ]
            ]
            jobData.select([ 'job_id' ],
                            from_ = [ 'kokkos_app' ],
                            where = where,
                            order_by = 'inst_job_app_time',
                            unique = True)
            res = jobData.get_results()
            result["columns"] = [ { "text" : "job_id" } ]
            result["rows"] = res.tolist()
            result["type"] = "table"
            return [ result ]
        except Exception as e:
            a, b, c = sys.exc_info()
            log.write('PapiLikeJobs '+str(e)+' '+str(c.tb_lineno))
            return None

    def getMeanPapiRankTable(self, schemaName, job_id):
        """Return mean papi metrics across ranks for a given job_id"""
        if not job_id:
            log.write('No job_id')
            return None
        xfrm, job = self.getPapiDerivedMetrics(job_id)
        xfrm.push(job)
        idx = job.series.index('tot_ins')
        series = job.series[idx:]
        xfrm.mean(series, group_name='rank', keep=job.series[0:idx-1], xfrm_suffix='')
        job = xfrm.pop()
        result = {}
        rows = []
        columns = []
        series_names = job.series
        idx = series_names.index('timestamp')
        del series_names[idx]
        idx = series_names.index('job_id')
        del series_names[idx]
        idx = series_names.index('component_id')
        del series_names[idx]
        for ser in series_names:
            columns.append({"text": ser})
        result['columns'] = columns
        for i in range(0, job.series_size):
            row = []
            for col in series_names:
                row.append(np.nan_to_num(job.array(col)[i]))
            rows.append(row)
        result["rows"] = rows
        result["type"] = "table"
        return [ result ]

    def getPapiDerivedMetrics(self, job_id, time_series=False, start=None, end=None):
        """Calculate derived papi metrics for a given job_id"""
        try:
            src = SosDataSource()
            src.config(cont=self.cont)
            if not time_series:
                src.select(['papi-events.*'],
                           from_ = [ 'papi-events' ],
                           where = [
                               [ 'job_id', Sos.COND_EQ, int(job_id) ]
                           ],
                           order_by = 'job_comp_time'
                )
            else:
                src.select(['papi-events.*'],
                    from_ = [ 'papi-events' ],
                    where = [
                        [ 'job_id', Sos.COND_EQ, int(job_id) ],
                        [ 'timestamp', Sos.COND_GE, start ],
                        [ 'timestamp', Sos.COND_LE, end ]
                    ],
                    order_by = 'job_comp_time'
                )
            job_comp_end = self.getJobCompEnd(int(job_id))
            job_comps = job_comp_end.array('component_id')
            comp_end = job_comp_end.array('job_end_max')
            nodes = job_comp_end.array('node_id')
            xfrm = Transform(src, None, limit=self.maxDataPoints)

            res = xfrm.begin(count=self.maxDataPoints)
            if res is None:
                # Job too short to record data
                print('no res')
                return (None, None)

            while res is not None:
                res = xfrm.next(count=self.maxDataPoints)
                if res is not None:
                    xfrm.concat()

            result = xfrm.pop()
            first = result.series.index('PAPI_TOT_INS')
            event_names = result.series[first:]
            cpu_count = int(result.array('PPN')[0])
            components = result.array('component_id')

            nda = np.ndarray([ result.series_size * cpu_count, 4 + len(event_names)])
            outrow = 0
            timestamps = result.array('timestamp')
            last_comp = None
            outcol = 4

            for name in event_names:
                series = result.array(name)
                outrow = 0
                for inrow in range(0, result.series_size):
                    comp_id = int(components[inrow])
                    comp = np.where(job_comps == comp_id)[0]
                    if len(comp) == 0:
                        # Some components may have not completed (i.e. jobinfo state ! = 2)
                        continue
                    if timestamps[inrow] >= \
                     dt.datetime.utcfromtimestamp(comp_end[comp[0]] - 1):
                        # Don't accept data from the component after the job has exited
                        continue
                    rank = nodes[comp[0]] * cpu_count
                    for cpu in range(0, cpu_count):
                        nda[outrow,0] = timestamps[inrow].astype(float)
                        nda[outrow,1] = comp_id
                        nda[outrow,2] = job_id
                        nda[outrow,3] = rank + cpu              # pseudo-rank
                        nda[outrow,outcol] = series[inrow][cpu]
                        outrow += 1
                outcol += 1

            if outrow == 0:
                del nda
                return (None, None)

            dataSet = DataSet()
            # "Normalize" the event names
            series_names = [ 'timestamp', 'component_id', 'job_id', 'rank' ] + event_names
            for name in series_names:
                if name in event_name_map:
                    idx = series_names.index(name)
                    series_names[idx] = event_name_map[name]
            col = 0
            for ser in series_names:
                dataSet.append_array(outrow, ser, nda[:,col])
                col += 1
            dataSet.set_series_size(outrow)

            derived_names = [ "tot_ins", "tot_cyc", "ld_ins", "sr_ins", "br_ins",
                              "fp_ops", "l1_icm", "l1_dcm", "l2_ica", "l2_tca",
                              "l2_tcm", "l3_tca", "l3_tcm" ]

            xfrm.push(dataSet)
            xfrm.diff([ 'timestamp'] + derived_names, group_name = 'rank', xfrm_suffix='_rate',
                      keep=[ 'timestamp', 'component_id', 'job_id', 'rank' ])
            rates = xfrm.top()

            # Convert the sample interval to seconds from uS
            rates.rename('timestamp_rate', 'bin_width')
            bin_width = rates['bin_width'] / 1000000.0

            # Strip the rate off the other series names
            for name in rates.series:
                if "_rate" in name:
                    rates.rename(name, name.replace("_rate", ""))
            data = xfrm.top()
            job = DataSet()
            for ser in [ 'timestamp', 'job_id',
                         'component_id', 'rank' ]:
                job <<= data[ser]

            # Normalize the input by dividing by the
            # sample-interval, i.e diff(timestamp)
            for ser in derived_names:
                job <<= data[ser] / data['bin_width'] >> ser

            # cpi = tot_cyc / tot_ins
            job <<= job['tot_cyc'] / job['tot_ins'] >> 'cpi'

            # memory accesses
            mem_acc = job['ld_ins'] + job['sr_ins'] >> 'mem_acc'

            # uopi = (ld_ins + sr_ins) / tot_ins
            job <<= mem_acc / job['tot_ins'] >> 'uopi'

            # l1_miss_rate = (l1_icm + l1_dcm) / tot_ins
            l1_tcm = job['l1_icm'] + job['l1_dcm']
            job <<=  l1_tcm / job['tot_ins'] >> 'l1_miss_rate'

            # l1_miss_ratio = (l1_icm + l1_dcm) / (ld_ins + sr_ins)
            job <<= l1_tcm / mem_acc >> 'l1_miss_ratio'

            # l2_miss_rate = l2_tcm / tot_ins
            job <<= job['l2_tcm'] / job['tot_ins'] >> 'l2_miss_rate'

            # l2_miss_ratio = l2_tcm / mem_acc
            job <<= job['l2_tcm'] / mem_acc >> 'l2_miss_ratio'

            # l3_miss_rate = l3_tcm / tot_ins
            job <<= job['l3_tcm'] / job['tot_ins'] >> 'l3_miss_rate'

            # l3_miss_ratio = l3_tcm / mem_acc
            job <<= job['l3_tcm'] / mem_acc >> 'l3_miss_ratio'

            # l2_bandwidth = l2_tca * 64e-6
            job <<= job['l2_tca'] * 64e-6 >> 'l2_bw'

            # l3_bandwidth = (l3_tca) * 64e-6
            job <<= job['l3_tca'] * 64e-6 >> 'l3_bw'

            # floating_point
            job <<= job['fp_ops'] / job['tot_ins'] >> 'fp_rate'

            # branch
            job <<= job['br_ins'] / job['tot_ins'] >> 'branch_rate'

            # load
            job <<= job['ld_ins'] / job['tot_ins'] >> 'load_rate'

            # store
            job <<= job['sr_ins'] / job['tot_ins'] >> 'store_rate'

            return (xfrm, job)
        except Exception as e:
            a, b, c = sys.exc_info()
            log.write('derivedMetrics '+str(e)+' '+str(c.tb_lineno))
            return None

    def getPapiRankStats(self, xfrm, job):
        try:
            """Return min/max/standard deviation/mean for papi derived metrics"""

            stats = DataSet()
            xfrm.push(job)
            events = job.series
            idx = events.index('rank')
            events = events[idx+1:]
            # compute the rank containing the minima for each event
            mins = DataSet()
            for name in events:
                xfrm.dup()
                xfrm.min([ name ], group_name='rank')
                xfrm.minrow(name+'_min')
                xfrm.top().rename('rank', name + '_min_rank')
                mins.append_series(xfrm.pop())

            # compute the rank containing the maxima for each event
            maxs = DataSet()
            for name in events:
                xfrm.dup()
                xfrm.max([ name ], group_name='rank')
                xfrm.maxrow(name+'_max')
                xfrm.top().rename('rank', name + '_max_rank')
                maxs.append_series(xfrm.pop())

            # compute the standard deviation
            xfrm.dup()
            xfrm.std(events)
            stats.append_series(xfrm.pop())

            # mean
            xfrm.mean(events)
            stats.append_series(xfrm.pop())

            return (events, mins, maxs, stats) 
        except Exception as e:
            a, b, c = sys.exc_info()
            log.write('papiRankStats '+str(e)+' '+str(c.tb_lineno))
            return str(e)

class Annotations(object):
    def __init__(self, cont):
        self.cont = cont

    def getJobMarkers(self, start, end, jobId=None, compId=None):
        """Query Job Marker annotations

        Positional Parameters:
        -- The start of the date/time range
        -- The end of the date/time range

        Keyword Parameters:
        jobId - Show only markers for the specified job
        compId - Show only markers for the specified component
        """
        src = SosDataSource()
        src.config(cont=self.cont)

        if jobId != None:
            # ignore the start/end time for the job markers
            jobId = int(jobId)
            where = [ [ 'job_id', Sos.COND_EQ, jobId ] ]
            by = 'job_comp_time'
        elif compId != None:
            where = [
                [ 'component_id', Sos.COND_EQ, compId ],
                [ 'timestamp', Sos.COND_GE, start ],
                [ 'timestamp', Sos.COND_LE, end ],
            ]
            by = 'timestamp'
        else:
            where = [
                [ 'timestamp', Sos.COND_GE, start ],
                [ 'timestamp', Sos.COND_LE, end ],
            ]
            by = 'timestamp'

        src.select([ 'job_id', 'job_start', 'job_end' ],
                       from_ = [ 'jobinfo' ],
                       where = where,
                       order_by = by
                   )
        x = Transform(src, None, limit=12384)
        res = x.begin()
        if not res:
            return res
        # x.top().show()
        result = x.dup()
        x.min([ 'job_start' ], group_name='job_id', xfrm_suffix='')
        result.concat(x.pop())
        x.max([ 'job_end' ], group_name='job_id', xfrm_suffix='')
        result.concat(x.pop())
        nda = result.array('job_start')
        nda *= 1000
        nda1 = result.array('job_end')
        nda1 *= 1000
        return result
