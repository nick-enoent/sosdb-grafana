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
            return {0}
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
        comps = src.get_df()
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
        src = SosDataSource()
        src.config(cont=cont)
        where_ = []
        where_.append(['job_id', Sos.COND_GT, 1])
        where_.append([ 'timestamp', Sos.COND_GT, start ])
        if end > 0:
            where_.append([ 'timestamp', Sos.COND_LE, end ])
        src.select([ 'job_id' ],
                   from_    = [ schema_name ],
                   where    = where_,
                   order_by = 'time_job_comp'
        )
        jobs = src.get_df(limit=8128)
        if jobs is None:
            return {0}
        job_ids = np.unique(jobs['job_id'])
        result = {}
        for job_id in job_ids:
            result[str(int(job_id))] = int(job_id)
        return result

class Query(object):
    def __init__(self, cont, schemaName, index='time_job_comp'):
        self.cont = cont
        self.schemaName = schemaName
        self.index = index
        self.maxDataPoints = 4096

    def getJobComponents(self, job_id):
        """Get components for a particular job"""
        src = SosDataSource()
        src.config(cont=self.cont)
        src.select([ 'component_id' ],
                   from_ = [ self.schemaName ],
                   where = [ [ 'job_id', Sos.COND_EQ, job_id ] ],
                   order_by = 'job_comp_time'
                   )
        res = src.get_df(limit=4096)
        if res:
            ucomps = np.unique(res['component_id'])
            return ucomps
        return None

    def getJobCompEnd(self, job_id):
        """Get job end"""
        src = SosDataSource()
        src.config(cont=self.cont)
        src.select([ self.schemaName+'.*' ],
                  from_    = [ self.schemaName ],
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

    def getComponents(self, start, end):
        """Return unique components with data for this schema"""
        src = SosDataSource()
        src.config(cont=self.cont)
        src.select([ 'component_id' ],
                   from_ = [ self.schemaName ],
                   where = [
                       [ timestamp, Sos.COND_GE, start ],
                       [ timestamp, Sos.COND_LE, end ]
                   ],
                   order_by = 'time_comp'
               )
        res = src.get_results(limit=4096)
        if res:
            ucomps = np.unique(res['component_id'])
            return ucomps
        return None

    def getCompTimeseries(self, compIds, metricNames,
                          start, end, intervalMs, maxDataPoints, jobId=0):
        """Return time series data for a particular component/s"""
        src = SosDataSource()
        src.config(cont=self.cont)

        if type(metricNames) != list:
            metricNames = [ metricNames ]
        result = []
        if compIds:
            if type(compIds) != list:
                compIds = [ int(compIds) ]
        elif jobId != 0:
            src.select([ 'component_id'],
                from_ = [ self.schemaName ],
                where = [ [ 'job_id', Sos.COND_EQ, jobId ] ],
                order_by = 'job_time_comp'
            )
            comps = src.get_df(limit=maxDataPoints)
            if comps.empty:
                compIds = np.zeros(1)
            else:
                compIds = np.unique(comps['component_id'])
        else:
            src.select([ 'component_id' ],
                from_ = [ self.schemaName ],
                where = [
                           [ 'timestamp', Sos.COND_GE, start ],
                           [ 'timestamp', Sos.COND_LE, end ],
                       ],
                       order_by = 'time_comp_job'
                   )
            comps = src.get_df(limit=maxDataPoints)
            if comps.empty:
                compIds = np.zeros(1)
            else:
                compIds = np.unique(comps['component_id'])
        for comp_id in compIds:
            for metric in metricNames:
                if comp_id != 0:
                    where_ = [
                        [ 'component_id', Sos.COND_EQ, comp_id ]
                    ]
                else:
                    where_ = []
                if jobId != 0:
                    self.index = "job_comp_time"
                    where_.append([ 'job_id', Sos.COND_EQ, int(jobId) ])
                else:
                    self.index = "time_comp"
                    where_.append([ 'timestamp', Sos.COND_GE, start ])
                    where_.append([ 'timestamp', Sos.COND_LE, end ])
                src.select([ metric, 'timestamp' ],
                           from_ = [ self.schemaName ],
                           where = where_,
                           order_by = self.index
                       )
                time_delta = end - start
                res = src.get_df(limit=100000)
                res['timestamp'] = res['timestamp'].values.astype(np.int64) / int(1e6)
                if res is None:
                    continue
                result.append({ "target" : '['+str(comp_id)+']'+metric, "datapoints" :
                                res.to_numpy().tolist() })
        return result

    def getTable(self, index, metricNames, start, end):
        src = SosDataSource()
        src.config(cont=self.cont)
        if self.schemaName == 'kokkos_app':
            src.select(metricNames,
                       from_ = [ self.schemaName ],
                       where = [
                           [ 'start_time', Sos.COND_GE, start ],
                       ],
                       order_by = 'time_job_comp'
            )
        else:
            src.select(metricNames,
                       from_ = [ self.schemaName ],
                       where = [
                           [ 'timestamp', Sos.COND_GE, start ],
                           [ 'timestamp', Sos.COND_LE, end ]
                       ],
                       order_by = "time_job_comp"
            )
        res = src.get_results()
        return res

    def papiGetLikeJobs(self, job_id, start, end):
        """Return jobs similar to requested job_id based on similar instance data"""
        try:
            src = SosDataSource()
            src.config(cont=self.cont)
            src.select(['inst_data'],
                        from_ = [ 'kokkos_app'],
                        where = [
                            [ 'job_id', Sos.COND_EQ, job_id ]
                        ],
                        order_by = 'job_comp_time',
            )
            res = src.get_results()
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
        by = 'comp_time'
        if jobId != None:
            # ignore the start/end time for the job markers
            jobId = int(jobId)
            where = [ [ 'job_id', Sos.COND_EQ, jobId ] ]
            by = 'job_rank_time'
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
            by = 'time_job'

        src.select([ 'job_id', 'job_start', 'job_end', 'component_id' ],
                       from_ = [ 'mt-slurm' ],
                       where = where,
                       order_by = by
                   )
        x = Transform(src, None, limit=12384)
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
