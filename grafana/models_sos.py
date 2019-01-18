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

    def getCompTimeseries(self, schemaName, compIds, metricNames,
                         timestamp, start, end, intervalMs, maxDataPoints):
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
                    while len(res.array('timestamp')) < maxDataPoints:
                        rs = src.get_results(inputer=inp, limit=maxDataPoints, interval_ms=intervalMs, reset=False)
                        if not len(rs.array('timestamp')):
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
            [ 'timestamp', Sos.COND_LE, end ]
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

    def getTable(self, schemaName, metricNames, start, end):
        src = SosDataSource()
        src.config(cont=self.cont)
        src.select(metricNames,
                   from_ = [ schemaName ],
                   where = [
                       [ 'timestamp', Sos.COND_GE, start ],
                       [ 'timestamp', Sos.COND_LE, end ]
                   ],
                   order_by = 'timestamp'
        )
        res = src.get_results()
        return res

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
