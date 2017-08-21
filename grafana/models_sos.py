from django.db import models
import datetime as dt
import os
import sys
from sosdb import Sos, bollinger
from sosgui import settings, logging
import numpy as np

log = logging.MsgLog("Grafana SOS")

class Object(object):
    pass

def SosErrorReply(err):
    return { "error" : "{0}".format(str(err)) }

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
             exc_a, exc_b, exc_tb = sys.exc_info()
             log.write("schema error: "+repr(e)+' '+repr(exc_tb.tb_lineno))

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
            pass

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
            log.write('query '+repr(query))
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

class SosQuery(SosRequest):
    """
    This is the base class for the SosTable class. It handles all of
    the query preparation, such as creating the filter, advancing to
    the first matching element, etc.
    """
    def __init__(self):
        super( SosQuery, self ).__init__()
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

        #
        # Open an iterator on the container
        #
        self.index_attr = None
        self.schema_name = self.schema().name()
        self.index_name = self.parms.index
        self.index_attr = self.schema().attr_by_name(self.index_name)
        self.tstamp = self.schema().attr_by_name('timestamp')
        self.comp_id_attr = self.schema().attr_by_name('component_id')
        self.iter_ = self.index_attr.index().stats()
        self.filt = Sos.Filter(self.index_attr)
        self.unique = False
        self.card = self.iter_['cardinality']
        if self.unique:
            self.card = self.card - self.iter_['duplicates']
        return (0, None)

class SosTable(SosQuery):
    def GET(self, request, comp_id):
        try:
            self.parse_query(request)
            #if rc != 0:
            #    return SosErrorReply(msg)
            #
            # Parse the select clause. The view_cols contains the index as it's first element.
            #
            self.view_cols = []
            self.met_lst = {}
            for attr in self.schema():
                self.met_lst[str(attr.name())] = str(attr.name())
            if self.parms.metric_select:
                self.metric_select = self.parms.metric_select
                for attr_name in self.metric_select.split(','):
                    if attr_name != self.index_name:
                        try:
                            a_name = self.met_lst[str(attr_name)]
                        except:
                            return [{ 'target':'Comp iD '+str(comp_id)+' '+i+' metric does not exist in schema', 'datapoints': [] }]
                        self.view_cols.append(a_name)
            else:
                for attr in self.schema():
                    if attr.name() != self.index_name:
                        self.view_cols.append(attr.name())
    
            obj = None
            if self.start == 0:
                obj = self.filt.begin()
                skip = self.start
            else:
                skip = float(self.end)
                self.filt.add_condition(self.comp_id_attr, Sos.COND_EQ, comp_id)
                self.filt.add_condition(self.tstamp, Sos.COND_GE, str(self.start))
                self.filt.add_condition(self.tstamp, Sos.COND_LE, str(self.end))
                #while obj and skip > obj.timestamp:
                #    obj = self.filt.next()
            ret_list = []
            for i in self.view_cols:
                attr_dict = {}
                max_count = 100000
                count, nda = self.filt.as_ndarray(max_count, shape=[i,self.index_name], order='index')
                nda_lst = nda[0:count-1].tolist()
                try:
                    nda_lst[1][0]
                except:
                    return [{ 'target':'Comp iD '+str(comp_id)+' '+i, 'datapoints': [] }]
                for t in nda_lst:
                    t[1] = t[1] * 1000
                attr_dict = { 'target': 'Comp ID '+str(comp_id)+' '+i, 'datapoints' : nda_lst }
                ret_list.append(attr_dict)
            if self.filt:
                del self.filt
            self.release()
            return ret_list
        except Exception as e:
            if self.filt:
                del self.filt
            exc_a, exc_b, exc_tb = sys.exc_info()
            log.write('SosTable Err: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            self.release()
            return SosErrorReply(e)

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
                    if attr.indexed() == True:
                        self.metrics[attr.name()] = attr.name()
            elif search_type == 'metrics':
                for attr in self.schema():
                    if attr.indexed() == True:
                        pass
                    else:
                        self.metrics[str(attr.name())] = str(attr.name())
            return self.metrics
        except Exception as e:
            exc_a, exc_b, exc_tb = sys.exc_info()
            log.write('TemplateData Err: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            return {'TemplateData Err' : repr(e)+repr(exc_tb.tb_lineno) }

class Derivative(SosQuery):
    def GET_DEV(self, request, comp_id):
        try:
            self.parse_query(request)
            #if rc != 0:
            #    return SosErrorReply(msg)
            #
            # Parse the select clause. The view_cols contains the index as it's first element.
            #
            self.view_cols = []
            self.met_lst = {}
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
                skip = self.start
            else:
                skip = float(self.end)
                self.filt.add_condition(self.comp_id_attr, Sos.COND_EQ, str(comp_id))
                self.filt.add_condition(self.tstamp, Sos.COND_GE, str(self.start))
                self.filt.add_condition(self.tstamp, Sos.COND_LE, str(self.end))

            ret_list = []
            for i in self.view_cols:
                dvx_lst = []
                attr_dict = {}
                max_count = 1024
                count, nda = self.filt.as_ndarray(max_count, shape=[i,self.index_name], order='attribute')
                if count < max_count:
                    nda = nda[:,range(count)].copy()
                k=0
                try:
                    nda[1][k]
                except:
                    return [{ 'target':'Comp ID '+str(comp_id)+' '+i, 'datapoints' : [] }]
                for t in nda[1]:
                    nda[1][k] = nda[1][k] * 1000
                    k += 1
                dvx_lst = np.gradient(nda[0])
                k = 0
                retLst = []
                for d in dvx_lst:
                    tup = []
                    tup.append(d)
                    tup.append(nda[1][k])
                    retLst.append(tup)
                    k += 1
                attr_dict = { 'target': 'Comp ID '+str(comp_id)+' '+i, 'datapoints' : retLst }
                ret_list.append(attr_dict)
            if self.filt:
                del self.filt
            self.release()
            return ret_list
        except Exception as e:
            if self.filt:
                del self.filt
            exc_a, exc_b, exc_tb = sys.exc_info()
            log.write('Derivative Err: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            self.release()
            return SosErrorReply(e)

class LeastSquares(SosQuery):
    def GET_LEAST_SQ(self, request, comp_id):
        try:
            self.parse_query(request)
            #if rc != 0:
            #    return SosErrorReply(msg)
            #
            # Parse the select clause. The view_cols contains the index as it's first element.
            #
            self.view_cols = []
            self.met_lst = {}
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
                skip = self.start
            else:
                skip = float(self.end)
                self.filt.add_condition(self.comp_id_attr, Sos.COND_EQ, str(comp_id))
                self.filt.add_condition(self.tstamp, Sos.COND_GE, str(self.start))
                self.filt.add_condition(self.tstamp, Sos.COND_LE, str(self.end))
            ret_list = []
            log.write('view_cols: '+repr(self.view_cols))
            for i in self.view_cols:
                attr_dict = {}
                max_count = 1024
                count, nda = self.filt.as_ndarray(max_count, shape=[i,self.index_name], order='attribute')
                if count < max_count:
                    nda = nda[:,range(count)].copy()
                x_diff = []
                try:
                    nda[1][0]
                except:
                    return [{ 'target':'Comp ID '+str(comp_id)+' '+i, 'datapoints': [] }]
                for t in nda[1]:
                    t = t - nda[1][0] 
                    x_diff.append(t)
                least_sq = np.polyfit(x_diff, nda[0], 3)
                fit_fn = np.poly1d(least_sq)
                k = 0
                retLst = []
                for d in x_diff:
                    tup = []
                    ysq = fit_fn(d)
                    tup.append(ysq)
                    tup.append(nda[1][k]*1000)
                    retLst.append(tup)
                    k += 1
                attr_dict = { 'target': 'Comp ID '+str(comp_id)+' '+i, 'datapoints' : retLst }
                ret_list.append(attr_dict)
            if self.filt:
                del self.filt
            log.write('del self.filt')
            self.release()
            return ret_list
        except Exception as e:
            if self.filt:
                del self.filt
            exc_a, exc_b, exc_tb = sys.exc_info()
            log.write('LeastSquares Err: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            self.release()
            return SosErrorReply(e)

class Log(SosQuery):
    def GET_LOG(self, metrics, comp_id):
        try:
            x = 0
            for i in metrics:
               metrics[x][0] = np.log(metrics[x][0])
               x += 1
            return metrics
        except Exception as e:
            exc_a, exc_b, exc_tb = sys.exc_info()
            log.write('Log Err: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            return SosErrorReply(e)

class BollingerBand(SosQuery):
    def GET_BOLL(self, request, comp_id):
        try:
            self.parse_query(request)
            #if rc != 0:
            #    return SosErrorReply(msg)
            #
            # Parse the select clause. The view_cols contains the index as it's first element.
            #
            self.view_cols = []
            self.met_lst = {}
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
                skip = self.start
            else:
                skip = float(self.end)
                self.filt.add_condition(self.comp_id_attr, Sos.COND_EQ, str(comp_id))
                self.filt.add_condition(self.tstamp, Sos.COND_GE, str(self.start))
                self.filt.add_condition(self.tstamp, Sos.COND_LE, str(self.end))

            ret_list = []
            for i in self.view_cols:
                attr_dict = {}
                max_count = 1024
                count, nda = self.filt.as_ndarray(max_count, shape=[i,self.index_name], order='attribute')
                if count < max_count:
                    nda = nda[:,range(count)].copy()
                k=0
                # nda[0] = metric values
                # nda[1] = timestamps
                try:
                    nda[1][k]
                except:
                    return [{ 'target':'Comp ID  '+str(comp_id)+' '+i, 'datapoints':[] }]
                for t in nda[1]:
                    nda[1][k] = nda[1][k] * 1000
                    k += 1
                b = bollinger.Bollinger_band()
                boll_dict = b.calculate(nda[1], nda[0])
                k = 60
                retLst = []
                upper = boll_dict['upperband']
                lower = boll_dict['lowerband']
                ma = boll_dict['ma']
                upperband = []
                lowerband = []
                moving_avg = []
                for d in upper:
                    tup = []
                    up = []
                    tup.append(nda[0][k])
                    tup.append(nda[1][k])
                    up.append(d)
                    up.append(nda[1][k])
                    upperband.append(up)
                    retLst.append(tup)
                    k += 1
                k = 60
                for d in lower:
                    low = []
                    low.append(d)
                    low.append(nda[1][k])
                    lowerband.append(low)
                    k += 1
                k = 60
                for d in ma:
                    mova = []
                    mova.append(d)
                    mova.append(nda[1][k])
                    moving_avg.append(mova)
                    k += 1
                attr_dict = { 'target': 'Comp ID '+str(comp_id)+' '+i, 'datapoints' : retLst }
                ret_list.append(attr_dict)
                ret_list.append({ 'target' : 'Upper Band', 'datapoints' : upperband })
                ret_list.append({ 'target' : 'Lower Band', 'datapoints' : lowerband })
                ret_list.append({ 'target' : 'Moving Avg', 'datapoints' : moving_avg })
            if self.filt:
                del self.filt
            self.release()
            return ret_list
        except Exception as e:
            if self.filt:
                del self.filt
            exc_a, exc_b, exc_tb = sys.exc_info()
            log.write('BollBand Err: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            self.release()
            return SosErrorReply(e)
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
