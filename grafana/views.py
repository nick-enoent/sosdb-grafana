from django.shortcuts import render
from django.http import HttpResponse, Http404, HttpResponseRedirect
from sosgui import logging, settings
import datetime
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

    def parse(self, request):
        try:
            targets = request['targets']
            time_range = request['range']
            self.query_type = targets[0]['query_type'].encode('utf-8')
            self.container = targets[0]['container'].encode('utf-8')
            self.schema = targets[0]['schema'].encode('utf-8')
            self.index = targets[0]['index'].encode('utf-8')
            self.ms = targets[0]['target'].encode('utf-8')
            self.comp_id = targets[0]['comp_id'].encode('utf-8')
            self.comp_id = self.comp_id.strip('{').strip('}')
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
        return int(time.mktime(datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S").timetuple()))

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
        if qs.query_source == 'sos':
            jresp = []
            if qs.query_type == 'metrics':
                for cid in qs.comp_id.split(','):
                    ret = metric_query(qs, cid)
                    for i in ret:
                        jresp.append(i)
                jresp = json.dumps(jresp)
                return HttpResponse(jresp, content_type='application/json')
            elif qs.query_type == 'derivative':
                for cid in qs.comp_id.split(','):
                    dvx = models_sos.Derivative()
                    ret = dvx.GET_DEV(qs, cid)
                    for i in ret:
                        jresp.append(i)
                jresp = json.dumps(jresp)
                return HttpResponse(jresp, content_type='application/json')
            elif qs.query_type == 'least_sq':
                for cid in qs.comp_id.split(','):
                    least_sq = models_sos.LeastSquares()
                    ret = least_sq.GET_LEAST_SQ(qs, cid)
                    for i in ret:
                        jresp.append(i)
                jresp = json.dumps(jresp)
                return HttpResponse(jresp, content_type='application/json')
            elif qs.query_type == 'bollinger':
                for cid in qs.comp_id.split(','):
                    least_sq = models_sos.BollingerBand()
                    ret = least_sq.GET_BOLL(qs, cid)
                    jresp = json.dumps(ret)
                return HttpResponse(jresp, content_type='application/json')
            elif qs.query_type == 'log':
                sosLog = models_sos.Log()
                sq = models_sos.SosTable()
                for cid in qs.comp_id.split(','):
                    metrics = sq.GET(qs, cid)
                    log_metrics = metrics[0]
                    try:
                        log_metrics['datapoints'][0]
                    except:
                        return metrics
                    log_metrics['datapoints'] = sosLog.GET_LOG(log_metrics['datapoints'], cid)
                    jresp.append(log_metrics)
                jresp = json.dumps(jresp)
                return HttpResponse(jresp, content_type='application/json')
            else:
                return HttpResponse('{ error : query_type not supported }', content_type='application/json')
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
