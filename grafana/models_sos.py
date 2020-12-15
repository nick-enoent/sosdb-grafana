from django.db import models
import datetime as dt
import time
import os, sys, traceback
from sosdb import Sos
from sosgui import settings, _log
from numsos.DataSource import SosDataSource
from graf_analysis.grafanaAnalysis import papiAnalysis
from . import views
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from math import sqrt, ceil
import numpy as np
import pandas as pd

log = _log.MsgLog("Grafana SOS")

job_status_str = {
    1 : "running",
    2 : "complete"
}

papi_metrics = [ 
    "PAPI_TOT_INS",
    "PAPI_TOT_CYC",
    "PAPI_LD_INS",
    "PAPI_SR_INS",
    "PAPI_BR_INS",
    "PAPI_FP_OPS",
    "PAPI_L1_ICM",
    "PAPI_L1_DCM",
    "PAPI_L2_ICA",
    "PAPI_L2_TCA",
    "PAPI_L2_TCM",
    "PAPI_L3_TCA",
    "PAPI_L3_TCM"
]

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

filter_op_map = {
    "start"   : Sos.COND_GE,
    "end"     : Sos.COND_LE,
    "comp_id" : Sos.COND_EQ,
    "job_id"  : Sos.COND_EQ,
    "user_id" : Sos.COND_EQ
}

filter_attr_map = {
    "start" : "timestamp",
    "end"   : "timestamp",
    "comp_id" : "component_id",
    "job_id"  : "job_id",
    "user_id" : "user_id"
}

class GrafanaRequest:
    def __init__(self, cont, schemaName, index):
        self.cont = cont
        self.schemaName = schemaName
        self.index = index
        self.maxDataPoints = 4096
        self.src = SosDataSource()
        self.src.config(cont=self.cont)

# Class to handle template variable query requests for schemas,
# metrics, components, and jobs
class Search(GrafanaRequest):
    def __init__(self, cont, schemaName, index='time_job_comp'):
        super().__init__(cont, schemaName, index)

    def getSchema(self):
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
            return {0}
        where = []
        if start > 0:
            where.append([ 'timestamp', Sos.COND_GE, start ])
        if end > 0:
            where.append([ 'timestamp', Sos.COND_LE, end ])
        self.src.select([ 'component_id' ],
                   from_    = [ schema_name ],
                   where    = where,
                   order_by = 'timestamp'
        )
        comps = self.src.get_df()
        if comps is None:
            return {0}
        comp_ids = np.unique(comps['component_id'])
        result = {}
        for comp_id in comp_ids:
            result[str(int(comp_id))] = int(comp_id)
        return result

    def getJobs(self, cont, schema_name, start, end):
        # method to retrieve unique job_ids
        schema = cont.schema_by_name(schema_name)
        attr = schema.attr_by_name("job_id")
        if attr is None:
            return None
        where_ = []
        where_.append(['job_id', Sos.COND_GT, 1])
        where_.append([ 'timestamp', Sos.COND_GT, start ])
        if end > 0:
            where_.append([ 'timestamp', Sos.COND_LE, end ])
        self.src.select([ 'job_id' ],
                        from_    = [ schema_name ],
                        where    = where_,
                        order_by = 'time_job_comp'
        )
        jobs = self.src.get_df(limit=8128)
        if jobs is None:
            return {0}
        job_ids = np.unique(jobs['job_id'])
        result = {}
        for job_id in job_ids:
            result[str(int(job_id))] = int(job_id)
        return result

class Query(GrafanaRequest):
    def __init__(self, cont, schemaName, index='time_job_comp'):
        super().__init__(cont, schemaName, index)

    def parseFilters(self, kwargs):
        # Construct "where" clause for SosDataSource
        # Remove null parameters, and assign filters to attributes present in schema
        self.checkIndex()
        _filters = {}
        for attr in kwargs:
            if attr in filter_op_map:
                if kwargs[attr] != None:
                    _filters[attr] = kwargs[attr]
        return _filters

    def checkIndex(self):
        # Check default index 'time_job_comp' exists in schema
        # Otherwise assign first indexed attribute with 'time' in the name
        self.schema_attrs =  []
        attrs = []
        for attr in self.cont.schema_by_name(self.schemaName):
            self.schema_attrs.append(attr.name())
            if attr.is_indexed():
                attrs.append(attr.name())
        if self.index in attrs:
            pass
        else:
            for attr in attrs:
                if 'time' in attr:
                    self.index = attr.name()
                    break

    def getJobComponents(self, job_id):
        """Get components for a particular job"""
        self.src.select([ 'component_id' ],
                        from_ = [ self.schemaName ],
                        where = [ [ 'job_id', Sos.COND_EQ, job_id ] ],
                        order_by = 'job_comp_time'
        )
        res = self.src.get_df(limit=10000)
        if res:
            ucomps = np.unique(res['component_id'])
            return ucomps
        return None

    def getJobCompEnd(self, job_id):
        """Get job end"""
        self.src.select([ self.schemaName+'.*' ],
                        from_ = [ self.schemaName ],
                        where = [ [ 'job_id', Sos.COND_EQ, job_id ],
                                  [ 'job_status', Sos.COND_EQ, 2 ]
                        ],
                        order_by = 'job_comp_time'
        )
        res = self.src.get_results()
        if res is None:
            return None
        xfrm = Transform(self.src, None, limit=4096)
        res = xfrm.begin()
        xfrm.max([ 'job_end' ], group_name='component_id')
        comp_time = xfrm.pop()
        nodes = np.arange(comp_time.get_series_size())
        comp_time.append_array(comp_time.get_series_size(), 'node_id', nodes)
        return comp_time

    def getComponents(self, start, end):
        """Return unique components with data for this schema"""
        self.checkIndex()
        self.src.select([ 'component_id' ],
                        from_ = [ self.schemaName ],
                        where = [
                            [ timestamp, Sos.COND_GE, start ],
                            [ timestamp, Sos.COND_LE, end ]
                        ],
                        order_by = self.index
        )
        res = self.src.get_results(limit=4096)
        if res:
            ucomps = np.unique(res['component_id'])
            return ucomps
        return None

    def getTimeseries(self, metrics, **kwargs):
        # Return a mean timeseries of given metric(s) over the bin_width
        try:
            self.schema = self.cont.schema_by_name(self.schemaName)
            filters_ = self.parseFilters(kwargs)
            where_ = []
            for attr in filters_:
                where_.append([filter_attr_map[attr], filter_op_map[attr], kwargs[attr]])
            self.src.select(metrics + [ 'timestamp' ],
                            from_=[ self.schemaName ],
                            where = where_,
                            order_by=self.index
            )
            data = self.src.get_df(limit=1000000, index='timestamp')
            if len(data) > self.maxDataPoints:
                bin_width = ceil((kwargs['end'] - kwargs['start']) / self.maxDataPoints)
                data['timestamp'] = pd.to_datetime(data['timestamp'])
                res = data.resample(str(bin_width)+'S').mean()
            else:
                res = data
            # Formatter expects series named timestamp
            res['timestamp'] = res.index
            return res
        except Exception as e:
            a, b, c = sys.exc_info()
            log.write("getTimeseries Error: {0} line no {1}".format(e, c.tb_lineno))
            return None

    def getCompTimeseries(self, metricNames,
                          start=0, end=0,
                          intervalMs=1000,
                          maxDataPoints=4096,
                          comp_id=None, job_id=None):
        """Return time series data for a particular component/s"""
        if type(metricNames) != list:
            metricNames = [ metricNames ]
        result = []
        if comp_id:
            if type(comp_id) != list:
                comp_id = [ int(comp_id) ]
        elif job_id:
            self.src.select([ 'component_id'],
                    from_ = [ self.schemaName ],
                    where = [ [ 'job_id', Sos.COND_EQ, job_id ] ],
                    order_by = 'job_time_comp'
            )
            comps = self.src.get_df(limit=10000)
            if comps.empty:
                comp_id = np.zeros(1)
            else:
                comp_id = np.unique(comps['component_id'])
        else:
            self.src.select([ 'component_id' ],
                    from_ = [ self.schemaName ],
                    where = [
                               [ 'timestamp', Sos.COND_GE, start ],
                               [ 'timestamp', Sos.COND_LE, end ],
                           ],
                           order_by = 'time_comp_job'
            )
            comps = self.src.get_df(limit=10000)
            if comps.empty:
                comp_id = np.zeros(1)
            else:
                comp_id = np.unique(comps['component_id'])
        for c_id in comp_id:
            for metric in metricNames:
                if c_id:
                    where_ = [
                        [ 'component_id', Sos.COND_EQ, c_id ]
                    ]
                else:
                    where_ = []
                if job_id:
                    self.index = "job_comp_time"
                    where_.append([ 'job_id', Sos.COND_EQ, int(job_id) ])
                else:
                    self.index = "time_comp"
                    where_.append([ 'timestamp', Sos.COND_GE, start ])
                    where_.append([ 'timestamp', Sos.COND_LE, end ])
                self.src.select([ metric, 'timestamp' ],
                                from_ = [ self.schemaName ],
                                where = where_,
                                order_by = self.index
                )
                time_delta = end - start
                res = self.src.get_df(limit=100000)
                res['timestamp'] = res['timestamp'].values.astype(np.int64) / int(1e6)
                if res is None:
                    continue
                result.append({ "target" : '['+str(c_id)+']'+metric, "datapoints" :
                                res.to_numpy().tolist() })
        return result

    def getTable(self, index, metricNames, start, end):
        if self.schemaName == 'kokkos_app':
            self.src.select(metricNames,
                           from_ = [ self.schemaName ],
                           where = [
                               [ 'start_time', Sos.COND_GE, start ],
                           ],
                           order_by = 'time_job_comp'
            )
        else:
            self.src.select(metricNames,
                            from_ = [ self.schemaName ],
                            where = [
                                [ 'timestamp', Sos.COND_GE, start ],
                                [ 'timestamp', Sos.COND_LE, end ]
                            ],
                            order_by = "time_job_comp"
        )
        res = self.src.get_results()
        return res

    def papiGetLikeJobs(self, job_id, start, end):
        """Return jobs similar to requested job_id based on similar instance data"""
        try:
            self.src.select(['inst_data'],
                            from_ = [ 'kokkos_app'],
                            where = [
                                [ 'job_id', Sos.COND_EQ, job_id ]
                            ],
                            order_by = 'job_comp_time',
            )
            res = self.src.get_results()
            if res is None:
                return None
            result = {}
            jobData = SosDataSource()
            jobData.config(cont=self.cont)
            where = [
                [ 'inst_data' , Sos.COND_EQ, res.array('inst_data')[0] ]
            ]
            jobData.select([ 'job_id', 'user_id', 'job_name' ],
                            from_ = [ 'kokkos_app' ],
                            where = where,
                            order_by = 'inst_job_app_time',
                            unique = True
            )
            res = jobData.get_results()
            result["columns"] = [ { "text" : "Job Id" }, { "text" : "User Id" }, { "text" : "Name" } ]
            result["rows"] = res.tolist()
            result["type"] = "table"
            return [ result ]
        except Exception as e:
            a, b, c = sys.exc_info()
            log.write('PapiLikeJobs '+str(e)+' '+str(c.tb_lineno))
            return None

class Annotations(GrafanaRequest):
    def __init__(self, cont, schemaName, index='time_job_comp'):
        super().__init__(cont, schemaName, index)

    def getJobMarkers(self, start, end, jobId=None, compId=None):
        """Query Job Marker annotations

        Positional Parameters:
        -- The start of the date/time range
        -- The end of the date/time range

        Keyword Parameters:
        jobId - Show only markers for the specified job
        compId - Show only markers for the specified component
        """
        if jobId != None:
            # ignore the start/end time for the job markers
            jobId = int(jobId)
            where = [ [ 'job_id', Sos.COND_EQ, jobId ] ]
            self.index = 'job_rank_time'
        elif compId != None:
            where = [
                [ 'component_id', Sos.COND_EQ, compId ],
                [ 'timestamp', Sos.COND_GE, start ],
                [ 'timestamp', Sos.COND_LE, end ],
            ]
        else:
            where = [
                [ 'timestamp', Sos.COND_GE, start ],
                [ 'timestamp', Sos.COND_LE, end ],
            ]
            self.index = 'time_job_comp'

        self.src.select([ 'job_id', 'job_start', 'job_end', 'component_id' ],
                       from_ = [ 'mt-slurm' ],
                       where = where,
                       order_by = self.index
        )
        x = Transform(self.src, None, limit=10000)
        res = x.begin()
        if not res:
            return res
        # x.top().show()
        result = x.dup()
        x.min([ 'job_start' ], group_name='job_id', keep=['component_id'], xfrm_suffix='')
        result.concat(x.pop())
        x.max([ 'job_end' ], group_name='job_id', keep=['component_id'], xfrm_suffix='')
        result.concat(x.pop())
        nda = result.array('job_start')
        nda *= 1000
        nda1 = result.array('job_end')
        nda1 *= 1000
        return result
