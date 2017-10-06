from django.contrib.auth import authenticate, login
from django.shortcuts import render
from django.http import HttpResponse, Http404, HttpResponseRedirect
from sosgui import logging, settings
import datetime as dt
import time
import sys
import models_sos
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
            log.write('container '+repr(self.container))
            self.schema = targets[0]['schema'].encode('utf-8')
            self.index = targets[0]['index'].encode('utf-8')
            self.ms = targets[0]['target'].encode('utf-8')
            self.comp_id = targets[0]['comp_id'].encode('utf-8')
            self.comp_id = self.comp_id.strip('{').strip('}')
            self.job_id = targets[0]['job_id'].encode('utf-8')
            self.metric_select = self.ms.strip('{').strip('}')
            # parse start/end times
            self.start_time = self.format_time(time_range['from'].encode('utf-8'))
            self.end_time = self.format_time(time_range['to'].encode('utf-8'))
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            log.write('parse error: '+repr(e)+' '+repr(exc_tb.tb_lineno))
            return HttpResponse({'parse err: '+repr(e)}, content_type='application/json')

    def format_time(self, t):
        split = t.split('T')
        t = split[0]+' '+split[1]
        t = t[:-5]
        return int(time.mktime(dt.datetime.strptime(t, "%Y-%m-%d %H:%M:%S").timetuple()))


def renderToResult(res_list, comp_id, count, res, cols, time_col):
    for col in range(0, len(cols)):
        if col == time_col:
            continue
        if count > 0:
            result = res[:,[col,time_col]].tolist()
        else:
            result = []
        res_dict = { 'target': 'Comp ID ' + str(comp_id) + ' ' + cols[col],
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
            return HttpResponse('{ error : query_source not supported }', content_type='application/json')

        if qs.query_type == 'metrics':
            model = models_sos.Metrics()
        elif qs.query_type == 'derivative':
            model = models_sos.Derivative()
        elif qs.query_type == 'bollinger':
            model = models_sos.BollingerBand()
        else:
            return HttpResponse('{ error : query_type not supported }', content_type='application/json')

        res_list = []
        try:
            for cid in qs.comp_id.split(','):
                (count, res, cols, time_col) = model.getData(qs, cid)
                renderToResult(res_list, cid, count, res, cols, time_col)
        except:
            (count, res, cols, time_col) = model.getData(qs, qs.comp_id)
            renderToResult(res_list, qs.comp_id, count, res, cols, time_col)
        return HttpResponse(json.dumps(res_list), content_type='application/json')

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        log.write('query error: '+repr(e)+' '+repr(exc_tb.tb_lineno))
        return HttpResponse({'query err: '+repr(e)}, content_type='application/json')

def search(request):
    try:
        req = json.loads(request.body)
        resp = models_sos.TemplateData()
        ret = resp.GET_ATTRS(req)
        jresp = json.dumps(ret)
        return HttpResponse(jresp, content_type='application/json')
    except Exception as e:
        log.write('search error: '+repr(e))
        return HttpResponse({'error: '+repr(e)}, content_type='application/json')

def annotations(request):
    try:
       req = json.loads(request.body)
       return HttpResponse('not_implemented', content_type='application/json')
    except:
        a, b, exc_tb = sys.exc_info()
        log.write('annotation err '+repr(e)+' '+repr(exc_tb.tb_lineno))
        return HttpResponse({'err: '+repr(e)}, content_type='application/json')
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
        return HttpResponse({'metric_query error' : repr(e)})
