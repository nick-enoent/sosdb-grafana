from django.contrib.auth import authenticate, login
from django.shortcuts import render
from django.http import HttpResponse, Http404, HttpResponseRedirect, QueryDict
from sosgui import logging, settings
from sosdb import Sos
import traceback as tb
import datetime as dt
import time
import sys
import models_sos
try:
    import models_baler
except:
    pass
import json

log = logging.MsgLog("Grafana Views ")
containers = {}

def get_container(cont_name):
    if cont_name in containers:
        return containers[cont_name]
    path = settings.SOS_ROOT + '/' + cont_name
    cont = Sos.Container(path)
    containers[cont_name] = cont
    return cont

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

# ^$
def ok(request):
    log.write('ok')
    return HttpResponse(status=200)

# ^query
def query(request):
    try:
        a = dt.datetime.now()
        body = request.body
        req = json.loads(body)

        date_range = req['range']
        start = parse_date_iso8601(date_range['from'])
        end = parse_date_iso8601(date_range['to'])
        intervalMs = req['intervalMs']
        maxDataPoints = req['maxDataPoints']

        startS = float(start.strftime('%s'))
        endS = float(end.strftime('%s'))

        startMs = startS * 1.0e3
        endMs = endS * 1.0e3

        # if end < start (e.g. 0) end is now, but clamp
        # to display window
        if endMs < startMs:
            endMs = startMs + (intervalMs * maxDataPoints)
        target = req['targets'][0]
        cont_name = str(target['container'])
        cont = get_container(cont_name)
        if cont is None:
            return HttpResponse("The container {0} could not be opened.".\
                                format(cont_name),
                                content_type="text/html")
        schemaName = str(target['schema'])
        metricNames = parse_glob(target['target'])
        if 'scopedVars' in req:
            scopedVars = req['scopedVars']
            if 'metric' in scopedVars:
                metric = scopedVars['metric']
                metricNames = [ str(metric['text' ]) ]
        if 'index' in target:
            index = str(target['index'])
        else:
            index = 'timestamp'
        if 'job_id' in target:
            jobId = int(target['job_id'])
        else:
            jobId = 0
        if 'comp_id' in target:
            compId = str(target['comp_id'])
            if ('{' in compId or ',' in compId):
                compId = parse_glob(compId)
            else:
                compId = int(compId)
        else:
            compId = None
        model = models_sos.Query(cont, schemaName, index)
        res_list = []
        if 'format' in target:
            fmt = target['format']
        else:
            fmt = "time_series"
        if fmt == 'job_table':
            columns, rows = model.getJobTable(0, start, end)
            if not columns:
                return [ { "columns" : [], "rows" : [], "type" : "table" } ]
            res_list = [ { "columns" : columns, "rows" : rows, "type": "table" } ]
        elif fmt == 'table':
            result = model.getTable(index,
                                    metricNames,
                                    start, end)
            if not result:
                return [ {"columns" : [], "rows" : [], "type" : "table" } ]
            columns = []
            for ser in result.series:
                columns.append({ "text" : ser })
            rows = result.tolist()
            res_list = [ { "columns" : columns, "rows" : rows, "type": "table" } ]
        elif fmt == 'papi_job_summary':
            res_list = model.getPapiSumTable(jobId, int(startS), int(endS))
        elif fmt == 'papi_rank_table':
            res_list = model.getMeanPapiRankTable(jobId, int(startS), int(endS))
        elif fmt == 'papi_timeseries':
            res_list = model.getPapiTimeseries(metricNames, jobId, int(startS),
                                               int(endS), intervalMs, maxDataPoints, comp_id=compId)
        elif fmt == 'like_jobs':
            res_list = model.papiGetLikeJobs(jobId, startS, endS)
        elif fmt == 'time_series':
            if jobId != 0:
                result = model.getJobTimeseries(jobId,
                                                metricNames,
                                                start, end,
                                                intervalMs,
                                                maxDataPoints, compIds=compId)
                if result:
                    for res in result:
                        res_list.append({ 'target' : str(jobId) +
                                          '[' + str(res['comp_id']) + ']:'
                                          + metricNames[0],
                                          'datapoints' : res['datapoints']})
                else:
                    res_list = [{ 'target' : str(jobId) + '[???]:' + metricNames[0],
                                          'datapoints' : [] }]

            else:
                result = model.getCompTimeseries(compId,
                                                 metricNames,
                                                 start, end,
                                                 intervalMs,
                                                 maxDataPoints)
                if result:
                    for res in result:
                        res_list.append({ 'target' : '[' + str(res['comp_id']) + ']'
                                          + str(res['metric']),
                                          'datapoints' : res['datapoints']})
                else:
                    res_list = [{ 'target' : '[???]:' + str(metricNames),
                                          'datapoints' : [] }]
        else:
            res_list = [ { "target" : "error",
                           "datapoints" : "unrecognized format {0}".format(fmt) } ]
        res_list = json.dumps(res_list)
        return HttpResponse(res_list, content_type='application/json')
    except Exception as e:
        log.write(tb.format_exc())
        log.write(e.message)
        return HttpResponse(json.dumps([]), content_type='application/json')


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

        model = models_sos.Search(cont)
        resp = {}

        schema = parms['schema']
        query = parms['query']
        if query.lower() != "schema" and schema is None:
            if schema is None:
                raise ValueError("Error: The 'schema' parameter is missing from the search.")
                return HttpResponse(json.dumps(["Error", "Schema is required"]), content_type='application/json')

        if query.lower() == "schema":
            resp = model.getSchema(cont)
        elif query.lower() == "index":
            resp = model.getIndices(cont, schema)
        elif query.lower() == "metrics":
            resp = model.getMetrics(cont, schema)
        elif query.lower() == "components":
            resp = model.getComponents(cont, schema, start, end)
        elif query.lower() == "jobs":
            resp = model.getJobs(cont, schema, start, end)

        return HttpResponse(json.dumps(resp), content_type = 'application/json')

    except Exception as e:
        a,b,c = sys.exc_info()
        log.write("search: {0}".format(e.message)+' '+str(c.tb_lineno))
        return HttpResponse(json.dumps(e.message.split(' ')),
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
            for row in range(0, jobs.get_series_size()):
                entry = {}
                job_id = str(jid[row])

                # Job start annotation
                job_start = int(jstart[row])
                entry['annotation'] = annotation
                entry["text"] = 'Job ' + job_id + ' started'
                entry["time"] = job_start
                entry["title"] = "Job Id " + job_id
                annotes.append(entry)

                # Job end annotation
                job_end = int(jend[row])
                if job_end > job_start:
                    entry = {}
                    entry['annotation'] = annotation
                    entry["text"] = 'Job ' + job_id + ' finished'
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
        return HttpResponse(json.dumps(annotes), content_type='application/json')

    except Exception as e:
        log.write(tb.format_exc())
        log.write(e.message)
        return HttpResponse(json.dumps(annotes), content_type='application/json')
