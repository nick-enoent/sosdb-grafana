from django.shortcuts import render
from django.db import models
import datetime as dt
import os
import sys
from sosgui import settings, logging
from baler import Bq

log = logging.MsgLog("grafana_baler")

NORM = ""
BLUE = "\033[34m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RESET = "\033[0m"
BOLD =  "\033[1m"
UNDERLINE = "\033[4m"

type_colors = {    
    Bq.BTKN_TYPE_TYPE : NORM,
    Bq.BTKN_TYPE_PRIORITY : NORM,
    Bq.BTKN_TYPE_VERSION : NORM,
    Bq.BTKN_TYPE_TIMESTAMP : BOLD + YELLOW,
    Bq.BTKN_TYPE_HOSTNAME : BOLD+GREEN,
    Bq.BTKN_TYPE_SERVICE : GREEN,
    Bq.BTKN_TYPE_PID : YELLOW,
    Bq.BTKN_TYPE_IP4_ADDR : NORM,
    Bq.BTKN_TYPE_IP6_ADDR : NORM,
    Bq.BTKN_TYPE_ETH_ADDR : NORM,
    Bq.BTKN_TYPE_HEX_INT : BOLD+BLUE,
    Bq.BTKN_TYPE_DEC_INT : BOLD+BLUE,
    Bq.BTKN_TYPE_FLOAT : BOLD+BLUE,
    Bq.BTKN_TYPE_PATH : BOLD+YELLOW,
    Bq.BTKN_TYPE_URL : BOLD+YELLOW,
    Bq.BTKN_TYPE_WORD : BOLD,
    Bq.BTKN_TYPE_SEPARATOR : NORM,
    Bq.BTKN_TYPE_WHITESPACE : NORM,
    Bq.BTKN_TYPE_TEXT : BOLD+RED,
}

def skip_token(tkn):
    return False
    if tkn.has_type(Bq.BTKN_TYPE_SEPARATOR):
        return True 
    elif tkn.has_type(Bq.BTKN_TYPE_WHITESPACE):
        return True 
    elif tkn.has_type(Bq.BTKN_TYPE_HEX_INT):
        return True 
    elif tkn.has_type(Bq.BTKN_TYPE_DEC_INT):
        return True 
    elif tkn.has_type(Bq.BTKN_TYPE_FLOAT):
        return True 
    return False

def ErrorReply(err):
    return { "error" : "{0}".format(str(err))}

def GetBstore(name):
    for store in settings.SYSLOG_CFG['stores']:
        if name == store['name']:
            path = str(store['path'])
            bs = Bq.Bstore()
            bs.open(path)
            return bs
    return None

def fmt_tkn_str(tkn):
    try:
        tkn_str = ""
        if tkn.tkn_id() in type_colors:
            tkn_str = type_colors[tkn.tkn_id()]
        elif tkn.first_type() in type_colors:
            tkn_str = type_colors[tkn.first_type()]
        if tkn.tkn_id() == Bq.BTKN_TYPE_WHITESPACE:
            tkn_str += " "
        else:
            tkn_str += tkn.ptn_tkn_str()
        return tkn_str
    except Exception as e:
        exc_a, exc_b, exc_tb = sys.exc_info()
        log.write('fmt_tkn_str err '+repr(e)+' '+repr(exc_tb.tb_lineno))

def tkn_from_str(bs, tkn_str):
    tkn = bs.tkn_by_name(tkn_str)
    if tkn:
        return tkn.tkn_id()
    else:
        raise NameError("No token with name {0}".format(req.tkn_str))

def BqMessageQuery(bs, start, end, compId, ptnId=0):
    ''' Queries for messages with Bq api
        Takes start/end time arguments'''
    if not bs:
        return { "messages" : [], "iTotalRecords" : 0, "iTotalDisplayRecords" : 0 }
    try:
        messages = {}
        msg_list = []
        for ptnid in ptnId:
            mi = Bq.Bmsg_iter(bs)
            mi.set_filter(comp_id=compId, ptn_id=ptnid, tv_begin=(start,0),tv_end=(end,0))
            for m in mi:
                if end > 0 and m.tv_sec() > end:
                    break
                msg_obj = {}
                tkn_list = []
                for tkn in m:
                    tkn_obj = {}
                    tkn_str = tkn.tkn_str()
                    tkn_obj['tkn_text'] = tkn.tkn_str()
                    tkn_list.append(tkn_obj)
                msg_obj['ptn_id'] = m.ptn_id()
                msg_obj['comp_id'] = m.comp_id()
                msg_obj['timestamp'] = m.tv_sec()
                msg_obj['tkn_list'] = tkn_list
                msg_list.append(msg_obj)
        if mi:
            del mi
        bs.close()
        messages['messages'] = msg_list
        return messages
    except Exception as e:
        e_type, e_obj, e_tb = sys.exc_info()
        log.write('BqMessageQuery: Line'+repr(e_tb.tb_lineno)+' '+repr(e))
        if bs:
            bs.close()
        return {'BqMessageQuery Error' : str(e) }

def MsgAnnotations(cont, start, end, compId, ptnId, ann):
    try:
        messages = BqMessageQuery(cont, start, end, compId, ptnId)
        annotations = []
        for m in messages['messages']:
            obj = {}
            tkn_str = ''
            for t in m['tkn_list']:
                tkn_str += str(t['tkn_text'])
            obj['annotation'] = ann
            obj["text"] = tkn_str
            obj["tags"] =  ["comp_id "+repr(int(m['comp_id']))]
            obj["time"] = m["timestamp"] * 1000
            obj["title"] = "Pattern ID "+repr(int(m['ptn_id']))
            annotations.append(obj)
        return annotations
    except Exception as e:
        e_type, e_obj, e_tb = sys.exc_info()
        log.write('MsgAnnotations: Line '+str(e_tb.tb_lineno)+' '+str(e))
        return { 'MsgAnnotations Error' : str(e) }

