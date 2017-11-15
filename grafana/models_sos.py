from django.db import models
import datetime as dt
import os, sys, traceback
from sosdb import Sos, bollinger
from sosgui import settings, logging
import views
import time
import numpy as np

log = logging.MsgLog("Grafana SOS")

class Object(object):
    pass

def SosErrorReply(err):
    return { "error" : repr(err) }

def open_test(path):
    try:
        c = Sos.Container(str(path))
        c.close()
        return True
    except Exception as e:
        log.write('open_test err: '+repr(e))
        return False

def SosDir():
    """
    Given the SOS_ROOT, search the directory structure to find all
    of the containers available for use. Note that even if the
    directory is there, this will skip the container if the
    requesting user does not have access rights.

    The OVIS store is organized like this:
    {sos_root}/{container_name}/{timestamped version}
    """
    rows = []
    try:
        dirs = os.listdir(settings.SOS_ROOT)
        # Check each subdirectory for files that constitute a container
        for ovc in dirs:
            try:
                ovc_path = settings.SOS_ROOT + '/' + ovc
                try:
                    files = os.listdir(ovc_path)
                    if '.__schemas.OBJ' in files:
                        if open_test(ovc_path):
                            rows.append( ovc )
                except Exception as e:
                    log.write(e)
            except Exception as e:
                log.write(e)
        return rows
    except Exception as e:
        # return render.table_json('directory', [ 'name' ], [], 0)
        return SosErrorReply(e)

class SosRequest(object):
    """
    This base class handles the 'container', 'encoding' and 'schema',
    'start', and 'count' keywords. For DataTables compatability, 'iDisplayStart'
    is a synonym of 'start' and 'iDisplayCount' is a synonym of 'count'.
    """
    JSON = 0
    TABLE = 1
    def __init__(self):
        self.encoding_ = self.JSON
        self.container_ = None
        self.schema_ = None
        self.start = 0
        self.count = 10

    def release(self):
        if self.container_:
            self.container_.close()
        self.container_ = None

    def __del__(self):
        self.release()

    def container(self):
        return self.container_

    def encoding(self):
        return self.encoding_

    def schema(self):
        return self.schema_

    def open_container(self, input_):
        #
        # Open the container or get it from our directory
        #
        self.input_ = input_
        try:
            self.container_ = Sos.Container(str(settings.SOS_ROOT + '/' + input_.container))
        except Exception as e:
            log.write(e)
            return {"The container {0} could not be opened" : repr(input_.container)}

        try:
            if self.container():
                self.schema_ = self.container().schema_by_name(input_.schema)
        except Exception as e:
            self.schema_ = None
            exc_a, exc_b, exc_tb = sys.exc_info()
            log.write('schema error: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            return { "Schema Error" : "Schema does not exist in Container" }

        #
        # iDisplayStart (dataTable), start
        #
        try:
            self.start = int(input_.start_time)
            self.end = int(input_.end_time)
        except Exception as e:
            self.start = 0
            self.end = None
        #
        # iDisplayLength (dataTables), count
        #
        try:
            self.count = self.end - self.start
        except Exception as e:
            pass
        # Job Id
        try:
            self.job_id = int(input_.job_id)
        except Exception as e:
            self.job_id = 0

class SosContainer(SosRequest):
    """
    Build up a container object that includes the container's schema,
    indexes and partitions
    """
    def GET(self, request):
        try:
            query = request.GET
            self.open_container(query)
            schema_rows = []
            for schema in self.container().schema_iter():
                row = { "Name" : schema.name(), "AttrCount" : schema.attr_count() }
                schema_rows.append(row)
            idx_rows = []
            for index in self.container().index_iter():
                stats = index.stats()
                row = { "Name" : index.name(),
                        "Entries" : stats['cardinality'],
                        "Duplicates" : stats['duplicates'],
                        "Size" : stats['size']}
                idx_rows.append(row)
            part_rows = []
            for part in self.container().part_iter():
                stat = part.stat()
                row = { "Name" : str(part.name()),
                        "State" : str(part.state()),
                        "Id" : int(part.part_id()),
                        "Size" : int(stat.size),
                        "Accessed" : str(stat.accessed),
                        #"Created" : stat['created'],
                        "Modified" : str(stat.modified)
                    }
                part_rows.append(row)
            return { "container" :
                     { "schema" : schema_rows,
                       "indexes" : idx_rows,
                       "partitions" : part_rows
                     }
                 }
        except Exception as e:
            exc_a, exc_b, exc_tb = sys.exc_info()
            log.write('SosContainer Err: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            return SosErrorReply(e)

class SosSchema(SosRequest):
    """
    Return all of the attributes and attribute meta-data for the
    specified schema.
    """
    def GET(self, request):
        try:
            query = request.GET
            self.open_container(query)
        except Exception as e:
            log.write(e)
            return SosErrorReply(e)
        if not self.schema():
            return SosErrorReply("A 'schema' clause must be specified.\n")
        rows = []
        for attr in self.schema():
            rows.append(str(attr.name()))
        log.write('rows '+repr(rows))
        return rows

class TemplateData(SosRequest):
    def GET_ATTRS(self, request):
        try:
            search_type = 'metrics'
            targets = request['target'].split('&')
            input_= Object()
            input_.container = targets[0]
            if len(targets) == 1:
                self.open_container(input_)
                self.schemas = {}
                for schema in self.container().schema_iter():
                    self.schemas[schema.name()] = schema.name()
                return self.schemas
            else:
                input_.schema = targets[1]
                self.open_container(input_)
            try:
                search_type = targets[2]
            except:
                search_type = 'metrics'
            self.metrics = {}
            if search_type == 'index':
                for attr in self.schema():
                    if attr.is_indexed() == True:
                        self.metrics[attr.name()] = attr.name()
            elif search_type == 'metrics':
                if not self.schema():
                    return {'"Error": "Error, Schema does not exist in container"'}
                for attr in self.schema():
                    if attr.is_indexed() != True:
                        self.metrics[attr.name()] = attr.name()
            elif search_type == 'jobs':
                if not self.schema():
                    return {'"Error": "Error, Schema does not exist in container"'}
                self.metrics = getJobs(self.container())
            return self.metrics
        except Exception as e:
            exc_a, exc_b, exc_tb = sys.exc_info()
            log.write('TemplateData Err: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            return {'TemplateData Err' : str(e) }

class SosQuery(SosRequest):
    """
    This is the base class for the SosTable class. It handles all of
    the query preparation, such as creating the filter, advancing to
    the first matching element, etc.
    """
    def __init__(self):
        SosRequest.__init__(self)
        self.filt = None

    def parse_query(self, request):
        self.parms = request
        try:
            self.open_container(self.parms)
        except Exception as e:
            return (1, "Exception in open_container: {0}".format(e), None)

        if not self.schema():
            return (1, "A 'schema' clause must be specified.\n", None)

        if not self.parms.index:
            return (1, "An 'index' clause must be specified.\n", None)

        self.maxDataPoints = int(request.maxDataPoints)
        self.intervalMs = float(request.intervalMs)
        #
        # Open an iterator on the container
        #
        self.index_attr = None
        self.schema_name = self.schema().name()
        self.index_name = self.parms.index
        self.index_attr = self.schema().attr_by_name(self.index_name)
        self.tstamp = self.schema().attr_by_name('timestamp')
        self.comp_id_attr = self.schema().attr_by_name('component_id')
        self.job_id_attr = self.schema().attr_by_name('job_id')
        self.iter_ = self.index_attr.index().stats()
        self.filt = Sos.Filter(self.index_attr)
        self.unique = False
        self.card = self.iter_['cardinality']
        if self.unique:
            self.card = self.card - self.iter_['duplicates']
        return (0, None)

class SosTable(SosQuery):
    def __init__(self):
        SosQuery.__init__(self)

    def getComponents(self, request, job_id):
        try:
            self.parse_query(request)
            filt = Sos.Filter(self.schema().attr_by_name('job_time_comp'))
            filt.add_condition(self.job_id_attr, Sos.COND_EQ, str(job_id))
            filt.add_condition(self.tstamp, Sos.COND_GE, str(self.start))
            filt.add_condition(self.tstamp, Sos.COND_LE, str(self.end))
            shape = [ 'component_id' ]
            count, nda = filt.as_ndarray(1024,
                                              shape=shape,
                                              order='index',
                                              interval_ms=self.intervalMs)
            comps = np.unique(nda)
            del filt
            self.release()
            return comps

        except Exception as e:
            if self.filt:
                del self.filt
            exc_a, exc_b, exc_tb = sys.exc_info()
            log.write('getData: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            log.write(traceback.format_tb(exc_tb))
            self.release()
            return ('err', {"target": +str(e), "datapoints" : []}, None, 0)

    def getData(self, request, job_id, comp_id):
        try:
            self.parse_query(request)
            self.view_cols = []
            self.met_lst = {}
            if not self.schema():
                return ('err', '{"target": "Schema does not exist in container", "datapoints" : [] }', None, 0)
            for attr in self.schema():
                self.met_lst[str(attr.name())] = str(attr.name())
            if self.parms.metric_select:
                self.metric_select = self.parms.metric_select
                for attr_name in self.metric_select.split(','):
                    if attr_name != self.index_name:
                        a_name = self.met_lst[str(attr_name)]
                        self.view_cols.append(a_name)
            else:
                for attr in self.schema():
                    if attr.name() != self.index_name:
                        self.view_cols.append(attr.name())

            obj = None
            if self.start == 0:
                obj = self.filt.begin()
            else:
                if comp_id != 0:
                    self.filt.add_condition(self.comp_id_attr, Sos.COND_EQ, str(comp_id))
                if job_id != 0:
                    self.filt.add_condition(self.job_id_attr, Sos.COND_EQ, str(job_id))
                self.filt.add_condition(self.tstamp, Sos.COND_GE, str(self.start))
                self.filt.add_condition(self.tstamp, Sos.COND_LE, str(self.end))
            shape = [ self.tstamp.name() ] + self.view_cols
            time_col = 0
            count, nda = self.filt.as_ndarray(self.maxDataPoints,
                                              shape=shape,
                                              order='index',
                                              interval_ms=self.intervalMs)
            if self.filt:
                del self.filt
            self.release()
            return (count, nda, shape, time_col)
        except Exception as e:
            if self.filt:
                del self.filt
            exc_a, exc_b, exc_tb = sys.exc_info()
            log.write('getData: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            log.write(traceback.format_tb(exc_tb))
            self.release()
            return (0, [], [], time_col)
            # return ('err', {"target": +str(e), "datapoints" : []}, None, 0)

def getJobs(cont):
        try:
            print("getJobs")
            schema = cont.schema_by_name('jobinfo')
            print("schema {0}".format(schema))
            start = schema.attr_by_name('job_start')
            print("start {0}".format(start))
            t = time.time()
            print("time {0}".format(t))
            filt = Sos.Filter(schema.attr_by_name('timestamp'))
            print("filt {0}".format(filt))
            filt.add_condition( start, Sos.COND_GE, str(t - 3600) )
            count, nda = filt.as_ndarray(1024, shape=[ 'job_id' ], order='index')
            jobs = np.unique(nda)
            print("jobs {0}".format(jobs))
            d = {}
            for job in jobs:
                if job == 0:
                    continue
                d[ str(int(job)) ] = int(job)
            del filt
            print d
            return d
        except Exception as e:
            print("Something bad {0}".format(str(e)))
            return []

class JobInfo(SosTable):
    def __init__(self):
        SosTable.__init__(self)

    def getData(self, request, job_id, comp_id):
        try:
            self.parse_query(request)
            self.view_cols = []
            self.met_lst = {}
            if not self.schema():
                return ('err', '{"target": "Schema does not exist in container", "datapoints" : [] }', None, 0)
            for attr in self.schema():
                self.met_lst[str(attr.name())] = str(attr.name())
            if self.parms.metric_select:
                self.metric_select = self.parms.metric_select
                for attr_name in self.metric_select.split(','):
                    if attr_name != self.index_name:
                        a_name = self.met_lst[str(attr_name)]
                        self.view_cols.append(a_name)
            else:
                for attr in self.schema():
                    if attr.name() != self.index_name:
                        self.view_cols.append(attr.name())

            job_start = self.schema().attr_by_name('job_start')
            obj = None
            if self.start != 0:
                self.filt.add_condition(job_start, Sos.COND_GE, str(self.start))
                self.filt.add_condition(job_start, Sos.COND_LE, str(self.end))
            obj = self.filt.begin()
            rows = []
            cols = []
            for col in self.view_cols:
                cols.append([])
            count = len(self.view_cols)
            job_id_idx = self.schema().attr_by_name('job_id').attr_id()
            job_start_idx = self.schema().attr_by_name('job_start').attr_id()
            job_end_idx = self.schema().attr_by_name('job_end').attr_id()
            job_user_idx = self.schema().attr_by_name('job_user').attr_id()
            job_exit_idx = self.schema().attr_by_name('job_exit_status').attr_id()
            job_name_idx = self.schema().attr_by_name('job_name').attr_id()
            id_col = {}
            end_col = {}
            name_col = {}
            user_col = {}
            exit_col = {}
            while obj:
                jid = obj[job_id_idx]
                ts = obj[job_start_idx] * 1000
                id_col[jid] = [ obj[job_id_idx], ts ]
                end_col[jid] = [ obj[job_end_idx] * 1000, ts ]
                name_col[jid] = [ obj[job_name_idx].tostring(), ts ]
                user_col[jid] = [ obj[job_user_idx].tostring(), ts ]
                exit_col[jid] = [ obj[job_exit_idx], ts ]
                obj = self.filt.next()

            ids = [ x[1] for x in id_col.items() ]
            ends = [ x[1] for x in end_col.items() ]
            names = [ x[1] for x in name_col.items() ]
            users = [ x[1] for x in user_col.items() ]
            exits = [ x[1] for x in exit_col.items() ]
            if self.filt:
                del self.filt
            self.release()
            return (count,
                    [ ids, ends, users, exits ],
                    [ 'job_id', 'job_end', 'job_user', 'job_exit' ], 0)

        except Exception as e:
            if self.filt:
                del self.filt
            exc_a, exc_b, exc_tb = sys.exc_info()
            log.write('getData: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            log.write(traceback.format_tb(exc_tb))
            self.release()
            return (0, [], [], 0)

class Derivative(SosTable):
    def __init__(self):
        SosTable.__init__(self)

    def getData(self, request, job_id, comp_id):
        (count, data, cols, time_col) = SosTable.getData(self, request, job_id, comp_id)
        if count > 0:
            res = data[0:count]
            res[:,time_col] *= 1000 # scale seconds to milliseconds
            for col in range(0, len(cols)):
                if col == time_col:
                    continue
                res[:,col] = np.gradient(res[:,col])
        else:
            res = []
        return (count, res, cols, time_col)

class Metrics(SosTable):
    def __init__(self):
        SosTable.__init__(self)

    def getData(self, request, job_id, comp_id):
        (count, data, cols, time_col) = SosTable.getData(self, request, job_id, comp_id)
        if count > 0:
            res = data[0:count]
            res[:,time_col] *= 1000 # scale seconds to milliseconds
        else:
            res = []
        return (count, res, cols, time_col)

class BollingerBand(SosTable):
    def __init__(self):
        SosTable.__init__(self)

    def getData(self, request, job_id, comp_id):
        (count, data, cols, time_col) = SosTable.getData(self, request, job_id, comp_id)
        if count > 0:
            res = data[0:count]
            res[:,time_col] *= 1000 # scale seconds to milliseconds
            b = bollinger.Bollinger_band()
            bb = b.calculate(res[:,1], res[:,0])
            lres = len(bb['upperband'])
            bbres = np.ndarray([lres, 5])
            win = bb['window']
            cols = ['timestamp', 'ma', 'upper', 'lower', cols[1]]
            bbres[:,0] = res[win:lres+win,0]
            bbres[:,1] = bb['ma']
            bbres[:,2] = bb['upperband']
            bbres[:,3] = bb['lowerband']
            bbres[:,4] = res[win:lres+win,1]
            count = lres
        else:
            bbres = []
        return (count, bbres, cols, time_col)

'''

class IndexAttrs(SosRequest):
    def GET_IDX_ATTRS(self, request):
        try:
            targets = request['target']
            input_ = Object()
            input_.container = targets[0]
            input_.schema = targets[1]
            self.open_container(input_)
            idx_iter = self.container().idx_iter()
            self.idx_attrs = []
            for i in idx_iter:
                if i.name().split('_', 0) == input_.container:
                    self.idx_attrs.append(i.name().split('_', 1))
                else:
                    pass
            return self.idx_attrs
        except Exception as e:
            exc_a, exc_b, exc_tb = sys.exc_info()
            log.write('IndexAttrs Err: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            return { 'IndexAttrs Err ': repr(e)+' '+repr(exc_tb.tb_lineno) }
'''
