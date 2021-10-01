from django.contrib.auth import authenticate, login
from django.shortcuts import render
from django.http import HttpResponse, Http404, HttpResponseRedirect, QueryDict
from django.views import View
from graf_analysis import grafanaFormatter
from sosgui import _log, settings
from sosdb import Sos
import traceback as tb
import datetime as dt
import _strptime
import pwd, sys, time
from . import models_sos
import numpy as np
import importlib
import json

try:
    import models_baler
except:
    pass

log = _log.MsgLog('grafana.views: ')

def converter(obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dt.datetime):
            return obj.__str__()

def close_container(cont):
    # both sos and dsos use the same method call to close a cont
    if cont is not None:
        cont.close()
        del cont

def parse_referer_date(s):
    if s == "now":
        return time.time()
    elif 'now' in s:
        w = s.split('-')
        now = time.time()
        n = float(w[1][:-1])
        u = w[1][-1]
        if u == 's':
            return now - n
        if u == 'm':
            return now - (n * 60)
        if u == 'h':
            return now - (n * 3600)
        if u == 'd':
            return now - (n * 86400)
        raise ValueError("No comprendez {0}".format(s))
    else:
        return float(s) / 1.0e3

def parse_glob(s):
    """parse '{this,that}' into an array of words ['this','that']"""
    names = s.replace('{','').replace('}','')
    ary = names.split(',')
    for i in range(0, len(ary)):
        ary[i] = str(ary[i])
    return ary

def parse_date_iso8601(s):
    return dt.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%fZ')

class QueryParameters(object):
    """Utility class for handling request body parameters"""
    def __init__(self, query_parms):
        terms = query_parms.split('&')
        self.params = {}
        for term in terms:
            if '=' in term:
                av = term.split('=')
                self.params[av[0]] = av[1]
            else:
                self.params[term] = True

    def __getitem__(self, idx):
        if idx in self.params:
            return self.params[idx]
        return None

    @property
    def count(self):
        return len(self.params)

    def contains(self, key):
        return key in self.params

########################################################################
# URL Handlers
########################################################################

class grafanaView(View):
    def __init__(self):
        super().__init__()
        self.dsos_req_handling = {
            'analysis' : self.get_analysis,
            'metrics'  : self.get_timeseries
        }

    def get_dsos_container(self):
        try:
            global log
            log = _log.MsgLog('get_dsos_container')
            cont_path = settings.DSOS_ROOT + '/' + self.targets[self.t_cnt]['container']
            self.dsos = Sos.Session(settings.DSOS_CONF)
            cont = self.dsos.open(cont_path)
            return cont
        except:
            return None

    def parse_params(self):
        date_range = self.req['range']
        start = parse_date_iso8601(date_range['from'])
        end = parse_date_iso8601(date_range['to'])
        self.startTS = int(start.strftime('%s'))
        self.endTS = int(end.strftime('%s'))
        self.intervalMs = self.req['intervalMs']
        self.interval = self.req['interval']
        self.maxDataPoints = self.req['maxDataPoints']
        # Each target represents an independent query
        self.targets = self.req['targets']
        # Limit formatter to first query format in request
        self.fmt = self.targets[0]['format']

    def parse_filters(self):
        self.filters = {}
        if self.targets[self.t_cnt]['filters'] is not None:
            for filter_ in self.targets[self.t_cnt]['filters']:
                filter_ = filter_.split('=')
                self.filters[filter_[0]] = filter_[1]
        else:
            self.filters = None

    def get_filters(self):
        if self.targets[self.t_cnt]['filters'] is not None:
            return self.targets[self.t_cnt]['filters']
        else:
            return []

    def post(self, request):
        body = request.body
        self.req = json.loads(body)
        self.parse_params()
        self.t_cnt = 0
        res = []
        self.cont = self.get_dsos_container()
        if self.cont is None:
            res = { "target" : "The container failed to open",  "datapoints" : [] }
            return HttpResponse(json.dumps(res, default=converter), content_type='application/json')
        for target in self.targets:
            self.parse_filters()
            result  = self.dsos_req_handling[target['query_type']]()
            self.t_cnt += 1
            if type(result) == list:
                for _res in result:
                    res.append(_res)
            else:
                res.append(result)
        close_container(self.cont)
        return HttpResponse(json.dumps(res, default=converter),
                            content_type='application/json')

    def get_uid(self):
        if self.targets[self.t_cnt]['user_name'] != None:
            pw = pwd.getpwnam(self.targets[self.t_cnt]['user_name'])
            user_id = pw.pw_uid
        else:
            user_id = 0
        return user_id

    def get_analysis(self):
        try:
            res = None
            module = importlib.import_module('graf_analysis.'+ self.targets[self.t_cnt]['analysis_module'])
            class_ = getattr(module, self.targets[self.t_cnt]['analysis_module'])
            model = class_(self.cont, int(self.startTS), int(self.endTS),
                           schema = self.targets[self.t_cnt]['schema'],
                           maxDataPoints = self.maxDataPoints)
            metrics = parse_glob(self.targets[self.t_cnt]['target'])
            res = model.get_data(metrics, self.get_filters())

            # Get formatter module
            fmtr_module = importlib.import_module('graf_analysis.'+self.fmt+'_formatter')
            fmtr_class = getattr(fmtr_module, self.fmt+'_formatter')
            fmtr = fmtr_class(res)
            res = fmtr.ret_json()
            return res

        except Exception as e:
            a, b, c = sys.exc_info()
            log.write(str(e)+' '+str(c.tb_lineno))
            res = {"target" : str(e), "datapoints" : [] }
            return res

    def get_timeseries(self):
        try:
            metrics = parse_glob(self.targets[self.t_cnt]['target'])
            model = models_sos.DSosQuery(self.cont,
                                     self.targets[self.t_cnt]['schema'],
                                     index='time_job_comp')
            res = model.getTimeseries(metrics,
                                      start=self.startTS, end=self.endTS,
                                      intervalMs=self.intervalMs,
                                      maxDataPoints=self.maxDataPoints,
                                      **(self.filters or {})
            )
            # Get formatter module
            fmtr_module = importlib.import_module('graf_analysis.'+self.fmt+'_formatter')
            fmtr_class = getattr(fmtr_module, self.fmt+'_formatter')
            fmtr = fmtr_class(res)
            res = fmtr.ret_json()
            return res
        except Exception as e:
            a, b, c = sys.exc_info()
            log.write(str(e)+' '+str(c.tb_lineno))
            res = { "target" : str(e),  "datapoints" : [] }
            return res

# ^$
def ok(request):
    log.write('ok')
    return HttpResponse(status=200)

# ^search
def search(request):
    try:
        body = request.body
        req = json.loads(body)

        if request.META.get('HTTP_REFERER') is not None:
            referer = request.META['HTTP_REFERER']
            query_dict = QueryDict(referer)
            if 'from' in query_dict:
                start = parse_referer_date(query_dict['from'])
            else:
                start = 0
            if 'to' in query_dict:
                end = parse_referer_date(query_dict['to'])
            else:
                end = 0
        else:
            start = 0
            end = 0

        # The first parameter in the target is the desired data:
        # - SCHEMA   Schema in the container:
        #    Syntax: query=schema&container=<cont_name>
        # - METRICS   Attrs in the schema
        #    Syntax: query=metrics&container=<cont_name>&schema=<schema_name>
        # - JOBS     Jobs with data in time range
        #    Syntax: query=jobs<schema>&container=<cont_name>&schema=<schema_name>
        # - COMPONENTS    Components with data
        #    Syntax: query=components&container=<cont_name>&schema=<schema_Name>
        parms = QueryParameters(req['target'])

        cont_name = parms['container']
        if cont_name is None:
            raise ValueError("Error: The 'container' key is missing from the search")

        cont = get_container(cont_name)
        if cont is None:
            raise ValueError("Error: The container {0} could not be opened.".format(cont_name))

        resp = {}

        schema = parms['schema']
        query = parms['query']
        model = models_sos.Search(cont, schema)
        if query.lower() != "schema" and schema is None:
            if schema is None:
                raise ValueError("Error: The 'schema' parameter is missing from the search.")
                return HttpResponse(json.dumps(["Error", "Schema is required"]), content_type='application/json')

        if query.lower() == "schema":
            resp = model.getSchema()
        elif query.lower() == "index":
            resp = model.getIndices()
        elif query.lower() == "metrics":
            resp = model.getMetrics()
        elif query.lower() == "components":
            resp = model.getComponents(start, end)
        elif query.lower() == "jobs":
            resp = model.getJobs(start, end)

        close_container(cont)
        return HttpResponse(json.dumps(resp), content_type = 'application/json')

    except Exception as e:
        a,b,c = sys.exc_info()
        log.write("search: {0}".format(e)+' '+str(c.tb_lineno))
        if not cont:
            pass
        else:
            close_container(cont)
        return HttpResponse(json.dumps(["Exception Error:", str(e)]),
                            content_type='application/json')

# ^annotations
def annotations(request):
    try:
        annotes = []
        body = request.body
        req = json.loads(body)

        date_range = req['range']
        start = parse_date_iso8601(date_range['from'])
        end = parse_date_iso8601(date_range['to'])

        annotation = req['annotation']
        query = annotation['query']
        parameters = QueryParameters(query)

        note_type = parameters['type']
        if note_type is None:
            raise ValueError("Missing type")

        cont_name = parameters['container']
        if cont_name is None:
            raise ValueError("Missing container name")

        jobId = parameters['job_id']
        compId = parameters['comp_id']
        if not parameters['ptn_id']:
            ptnId = [0]
        else:
            ptnId = parameters['ptn_id'].split(',')
            x = 0
            for i in ptnId:
                ptnId[x] = int(ptnId[x])
                x += 1
        if note_type == 'JOB_MARKERS':
            cont = get_container(cont_name)
            if cont is None:
                raise ValueError("Container '{0}' could not be opened.".format(cont_name))
            model = models_sos.Annotations(cont=cont)
            jobs = model.getJobMarkers(start, end, jobId=jobId, compId=compId)
            if jobs is None:
                raise ValueError("No data returned for jobId {0} compId {1} start {2} end {3}".\
                                 format(jobId, compId, start, end))
            jid = jobs.array('job_id')
            jstart = jobs.array('job_start')
            jend = jobs.array('job_end')
            jcomps = jobs.array('component_id')
            for row in range(0, jobs.get_series_size()):
                entry = {}
                job_id = str(jid[row])
                comp_id = str(jcomps[row])
                # Job start annotation
                job_start = int(jstart[row])
                entry['annotation'] = annotation
                entry["text"] = 'Job ' + job_id + ' started on node '+comp_id
                entry["time"] = job_start
                entry["title"] = "Job Id " + job_id
                annotes.append(entry)

                # Job end annotation
                job_end = int(jend[row])
                if job_end > job_start:
                    entry = {}
                    entry['annotation'] = annotation
                    entry["text"] = 'Job ' + job_id + ' finished on node '+comp_id
                    entry["time"] = job_end
                    entry["title"] = "Job Id " + job_id
                annotes.append(entry)
        elif note_type == 'LOGS':
            cont = models_baler.GetBstore(cont_name)
            if cont is None:
                raise ValueError("Container '{0}' could not be opened.".format(cont_name))
            start = int(start.strftime('%s'))
            end = int(end.strftime('%s'))
            annotes = models_baler.MsgAnnotations(cont, start, end, int(compId), ptnId, annotation)
        else:
            raise ValueError("Unrecognized annotation type '{0}'.".format(note_type))
        close_container(cont)
        return HttpResponse(json.dumps(annotes), content_type='application/json')

    except Exception as e:
        log.write(tb.format_exc())
        log.write(str(e))
        close_container(cont)
        return HttpResponse(json.dumps(annotes), content_type='application/json')
