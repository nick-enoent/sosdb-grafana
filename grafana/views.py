from django.contrib.auth import authenticate, login
from django.shortcuts import render
from django.http import HttpResponse, Http404, HttpResponseRedirect
from sosgui import logging, settings
import datetime as dt
import time
import sys
import models_sos
import models_baler
import json

log = logging.MsgLog("Grafana Views ")

def ok(request):
    return HttpResponse(status=200)

'''
class query_baler(object):
    def __init__(self):
        self.store = None
        self.container = None
        self.schema = None
        self.metric = None
        self.index = 'timestamp'
        self.query_type = None
        self.start_time = 0
        self.end_time = 0
        self.ptn_id = 0
        self.bin_width = 86400

    def parse(self, request):
        targets = request['targets']
        time_range = request['range']
        self.query_type = targets[1]['target'].encode('utf-8')
        self.store = targets[2]['target'].encode('utf-8')
        self.ptn_id = int(targets[3]['target'].encode('utf-8'))
        self.bin_width = int(targets[4]['target'].encode('utf-8'))
        # parse start/end times
        self.start_time = self.format_time(time_range['from'].encode('utf-8'))
        self.end_time = self.format_time(time_range['to'].encode('utf-8'))

    def format_time(self, t):
        split = t.split('T')
        t = split[0]+' '+split[1]
        t = t[:-5]
        return int(time.mktime(datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S").timetuple()))
'''

class annotation_query(object):
    def __init__(self):
        self.store = None
        self.job_id = 0
        self.comp_id = 0
        self.start_time = 0
        self.end_time = 0
        self.ptn_id = 0

    def parse(self, request):
        try:
            time_range = request['range']
            self.annotation = request['annotation']
            self.ann_query = self.annotation['query'].split('&')
            self.store = self.ann_query[0]
            self.ptn_id = self.ann_query[1].split(",")
            self.job_id = int(self.ann_query[2])
            self.comp_id = int(self.ann_query[3])
            self.start_time = format_time(time_range['from'].encode('utf-8'))
            self.end_time = format_time(time_range['to'].encode('utf-8'))

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            log.write('annotation parse error: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            return HttpResponse({'annotation parse err: '+str(e)}, content_type='application/json')


class query_sos(object):
    def __init__(self):
        self.store = None
        self.container = None
        self.schema = None
        self.metric_select = None
        self.index = 'timestamp'
        self.query_source = 'sos'
        self.query_type = 'metrics'
        self.start_time = 0
        self.end_time = 0
        self.comp_id = 0
        self.job_id = 0

    def parse(self, request):
        #if 'basicAuthUser' in request:
        #    username = request.POST['basicAuthUser']
        #    password = request.POST['basicAuthPassword']
        #    try:
        #        user = authenticate(request, username=username, password=password)
        #        if user is not None:
        #            login(request,user)
        #            return 1
        #        else:
        #            return 0
        #    except Exception as e:
        #        log.write('authenticate err: '+repr(e))
        #else:
        #    return 0
        if 'maxDataPoints' in request:
            self.maxDataPoints = request['maxDataPoints']
        else:
            self.maxDataPoints = 1024
        if 'intervalMs' in request:
            self.intervalMs = request['intervalMs']
        else:
            self.intervalMs = 1000
        try:
            targets = request['targets']
            self.targets = targets
            time_range = request['range']
            self.query_type = targets[0]['query_type'].encode('utf-8')
            self.container = targets[0]['container'].encode('utf-8')
            self.schema = targets[0]['schema'].encode('utf-8')
            self.index = targets[0]['index'].encode('utf-8')
            self.ms = targets[0]['target'].encode('utf-8')
            self.comp_id = targets[0]['comp_id'].encode('utf-8')
            self.comp_id = self.comp_id.strip('{').strip('}')
            self.job_id = targets[0]['job_id'].encode('utf-8')
            self.job_id = self.job_id.strip('{').strip('}')
            self.metric_select = self.ms.strip('{').strip('}')
            # parse start/end times
            self.start_time = format_time(time_range['from'].encode('utf-8'))
            self.end_time = format_time(time_range['to'].encode('utf-8'))
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            log.write('parse error: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            return HttpResponse({'parse err: '+str(e)}, content_type='application/json')

def format_time(t):
    split = t.split('T')
    t = split[0]+' '+split[1]
    t = t[:-5]
    return int(time.mktime(dt.datetime.strptime(t, "%Y-%m-%d %H:%M:%S").timetuple()))

def renderToResult(res_list, job_id, comp_id, count, res, cols, time_col):
    for col in range(0, len(cols)):
        if col == time_col:
            continue
        if count > 0:
            result = res[:,[col,time_col]].tolist()
        else:
            result = []
        if job_id == 0:
            res_dict = { 'target': 'Comp ID ' + str(comp_id) + ' ' + cols[col],
                         'datapoints' : result }
        else:
            if comp_id == 0:
                res_dict = { 'target': 'Job ID ' + str(job_id) + ' ' + cols[col],
                             'datapoints' : result }
            else:
                res_dict = { 'target': 'Job ID ' + str(job_id) +
                             ' Comp ID ' + str(comp_id) + ' ' + cols[col],
                             'datapoints' : result }
        res_list.append(res_dict)

def query(request):
    try:
        req = json.loads(request.body)
        #query_source = req['targets'][0]['target'].encode('utf-8')
        #if query_source == 'baler':
        #    qb = query_baler()
        #    qb.parse(req)
        #    if qb.query_type == 'ptn_hist':
        #        jresp = ptn_hist(qb)
        #        return HttpResponse(jresp, content_type='application/json')
        #    elif qb.query_type == 'patterns':
        #        jresp = patterns(query)
        #        return HttpResponse(jresp, content_type='application/json')
        #    else:
        #        return HttpResponse('{ error : query_type not supported }', content_type='application/json')
        qs = query_sos()
        qs.parse(req)
        #if not qs.parse(req):
        #    return HttpResponse({'Authentication Error: Incorrect user or password'}, content_type='application/json')
        if qs.query_source != 'sos':
            return HttpResponse('{ "error" : "query_source not supported" }', content_type='application/json')

        print("schema {0}".format(qs.schema))
        if qs.schema == 'jobinfo':
            model = models_sos.JobInfo()
            (count, res, cols, time_col) = model.getData(qs, 0, 0)
            print(count, cols, time_col, len(res), len(cols))
            res_list = []
            for col in range(0, len(res)):
                res_dict = { 'target': cols[col], 'datapoints' : res[col] }
                res_list.append(res_dict)
            return HttpResponse(json.dumps(res_list),
                                content_type='application/json')
        elif qs.query_type == 'metrics':
            model = models_sos.Metrics()
        elif qs.query_type == 'derivative':
            model = models_sos.Derivative()
        elif qs.query_type == 'bollinger':
            model = models_sos.BollingerBand()
        else:
            return HttpResponse('{ "error" : "query_type not supported" }',
                                content_type='application/json')

        res_list = []
        try:
            comp_list = qs.comp_id.split(',')
        except Exception as e:
            comp_list = [0]
        try:
            job_list = qs.job_id.split(',')
        except:
            job_list = [0]
        if job_list[0] == 0:
            for cid in comp_list:
                try:
                    cid = int(cid)
                except:
                    cid = 0
                (count, res, cols, time_col) = model.getData(qs, 0, cid)
                renderToResult(res_list, 0, cid, count, res, cols, time_col)
        else:
            for jid in job_list:
                try:
                    jid = int(jid)
                except:
                    jid = 0
                if comp_list[0] == '0' or comp_list[0] == 0:
                    comp_list = model.getComponents(qs, jid)
                    print("comp_list {0}".format(comp_list))
                for cid in comp_list:
                    if cid == 0 and len(comp_list) > 1:
                        continue
                    try:
                        cid = int(cid)
                    except:
                        cid = 0
                    (count, res, cols, time_col) = model.getData(qs, jid, cid)
                    if count == 'err':
                        log.write('res '+repr(res))
                        res = str(res)
                        return HttpResponse(json.dumps(res), content_type='application/json')
                    renderToResult(res_list, jid, cid, count, res, cols, time_col)
        return HttpResponse(json.dumps(res_list), content_type='application/json')

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        log.write('query error: '+repr(e)+' '+repr(exc_tb.tb_lineno))
        return HttpResponse(json.dumps({"target": "badness", "datapoints" : []}), content_type='application_json')
        # return HttpResponse(json.dumps({"target": '"' + str(e) + '"', "datapoints" : []}), content_type='application_json')

def search(request):
    try:
        req = json.loads(request.body)
        resp = models_sos.TemplateData()
        req['request'] = request
        ret = resp.GET_ATTRS(req)
        jresp = json.dumps(ret)
        return HttpResponse(jresp, content_type='application/json')
    except Exception as e:
        log.write('search error: '+repr(e))
        return HttpResponse('{"error" : '+'"' + str(e) + '"}', content_type='application/json')

def annotations(request):
    try:
       req = json.loads(request.body)
       aq = annotation_query()
       aq.parse(req)
       ret = models_baler.MsgAnnotations(aq)
       jresp = json.dumps(ret)
       return HttpResponse(jresp, content_type='application/json')
    except Exception as e:
        a, b, exc_tb = sys.exc_info()
        log.write('annotation err '+repr(e)+' '+repr(exc_tb.tb_lineno))
        return HttpResponse({'err: '+str(e)}, content_type='application/json')
'''
def ptn_hist(qb):
    try:
        resp = models_baler.Bq_Ptn_Hist(qb)
        jresp = json.dumps(resp)
        return jresp
    except Exception as e:
        log.write('ptn_hist: '+repr(e))
        return HttpResponse({'ptn_hist error' : repr(e)})

def patterns(query):
    resp = models_baler.BqPatternQuery(query)
    jresp = dumps(resp)
    return jresp

def messages(request):
    try:
        query_str = request.GET
        resp = models_baler.BqMessageQuery(query)
        jresp = json.dumps(resp)
        return HttpResponse(jresp, content_type='text/json')
    except Exception as e:
        log.write('Grafana messages error: '+repr(e))
        return HttpResponse({"Messsage Error": repr(e)})
'''

def metric_query(qs, comp_id):
    try:
        sq = models_sos.SosTable()
        resp = sq.GET(qs, comp_id)
        return resp
    except Exception as e:
        log.write('metric_query error: '+repr(e))
        return HttpResponse(json.dumps({ "error" : str(e) }))
