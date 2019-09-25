from django.db import models
import datetime as dt
import time
import os, sys, traceback, operator
from sosdb import Sos
from sosdb.DataSet import DataSet
from sosgui import settings, logging
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from models_sos import *
import numpy as np
import pandas as pd
from numsos import grafana
import views
import time

log = logging.MsgLog("Grafana Analysis")

class Analysis(Query):
    def compMinMeanMax(self, metric,
                       start, end,
                       interval,
                       maxDataPoints,
                       jobId):
        if jobId == 0:
            return [{ 'target' : 'Error: Please specify valid job_id', "datapoints" : [] }]
        src = SosDataSource()
        src.config(cont=self.cont)
        metric = metric[0]
        result = []
        try:
            src.select([ 'component_id' ],
                       from_ = [ self.schemaName ],
                       where = [
                           [ 'timestamp', Sos.COND_GE, start ],
                           [ 'timestamp', Sos.COND_LE, end ],
                       ],
                       order_by = 'time_comp_job'
                   )
            comps = src.get_results(limit=maxDataPoints)
            if not comps:
                return [{ 'target' : '[comp_id not found]:' + str(metric),
                          'datapoints' : [] }]
            else:
                compIds = np.unique(comps['component_id'].tolist())
            result = []
            datapoints = []
            src.select(['job_start', 'job_end' ],
                        from_ = [ 'mt-slurm' ],
                        where = [[ 'job_id', Sos.COND_EQ, jobId ]],
                        order_by = 'job_rank_time'
                )
            job_ts = src.get_results()
            if job_ts is None:
                return None
            job_start = job_ts.array('job_start')[0]
            job_end = job_ts.array('job_end')[0]
            for comp_id in compIds:
                where_ = [
                    [ 'component_id', Sos.COND_EQ, comp_id ]
                ]
                self.index = "job_comp_time"
                where_.append([ 'job_id', Sos.COND_EQ, int(jobId) ])
                src.select([ metric, 'timestamp' ],
                           from_ = [ self.schemaName ],
                           where = where_,
                           order_by = self.index
                       )
                inp = None
                res = src.get_df(limit=maxDataPoints)
                if res is None:
                    continue
                # get timestamp of job start/end
                start_d = dt.datetime.utcfromtimestamp(job_start).strftime('%m/%d/%Y %H:%M:%S')
                end_d = dt.datetime.utcfromtimestamp(job_end).strftime('%m/%d/%Y %H:%M:%S')
                ts = pd.date_range(start=start_d, end=end_d, periods=len(res.values[0].flatten()))
                series = pd.DataFrame(res.values[0].flatten(), index=ts)
                rs = series.resample('S').ffill()
                datapoints.append(rs.values.flatten())
                tstamp = rs.index
            i = 0
            tstamps = []
            while i < len(tstamp):
                ts = pd.Timestamp(tstamp[i])
                ts = np.int_(ts.timestamp()*1000)
                tstamps.append(ts)
                i += 1
            min_ = DataSet()
            i = 1
            min_datapoints = np.min(datapoints, axis=0)
            min_.append_array(len(min_datapoints), 'min_'+metric, min_datapoints)
            min_.append_array(len(tstamps), 'timestamp', tstamps)
            result.append({"target" : "min_"+str(metric), "datapoints" : min_.tolist() })
            mean = DataSet()
            mean_datapoints = np.mean(datapoints, axis=0)
            mean.append_array(len(mean_datapoints), 'mean_'+metric, mean_datapoints)
            mean.append_array(len(tstamps), 'timestamp', tstamps)
            result.append({"target" : "mean_"+str(metric), "datapoints" : mean.tolist() })
            max_ = DataSet()
            max_datapoints = np.max(datapoints, axis=0)
            max_.append_array(len(max_datapoints), 'max_'+metric, max_datapoints)
            max_.append_array(len(tstamps), 'timestamp', tstamps)
            result.append({"target" : "max_"+str(metric), "datapoints" : max_.tolist() })
            return result
        except Exception as e:
            a, b, c = sys.exc_info()
            log.write(str(e)+' '+str(c.tb_lineno))
            return None

    def rankMemByJob(self, start=None, end=None,
                     threshold=1.0, low_not_high=False, idle=False,
                     summary=False, job_id=False):
        try:
            src = SosDataSource()
            src.config(cont=self.cont)

            if job_id:
                where = [ [ 'job_id', Sos.COND_EQ, int(job_id) ] ]
                by = 'job_comp_time'
            elif idle:
                where = [ [ 'job_id', Sos.COND_EQ, 0 ] ]
                by = 'time_comp'
            else:
                where = [ [ 'job_id', Sos.COND_GE, 1 ] ]
                by = 'time_job_comp'
            where.append( [ 'timestamp', Sos.COND_GE, start ] )
            where.append( [ 'timestamp', Sos.COND_LE, end ] )

            src.select([ 'job_id', 'component_id', 'timestamp', 'MemTotal', 'MemFree' ],
                       from_ = [ 'meminfo' ],
                       where = where,
                       order_by = by)
            xfrm = Transform(src, None)
            res = xfrm.begin()
            if res is None:
                return None
            while res is not None:
                res = xfrm.next()
                if res is not None:
                    # concatenate TOP and TOP~1
                    xfrm.concat()
            
            data = xfrm.top()
            memUsedRatio = ((data['MemTotal'] - data['MemFree']) / data['MemTotal']) * 100 >> 'Mem_Used_Ratio'
            std = memUsedRatio.std()
            mean = memUsedRatio.mean()

            # Add timestamp, job_id, and component_id to the memUsedRatio data set
            memUsedRatio <<= data['timestamp']
            memUsedRatio <<= data['job_id']
            memUsedRatio <<= data['component_id']

            f = grafana.DataSetFormatter()
            if summary:
                xfrm.push(memUsedRatio)
                res = xfrm.min([ 'Mem_Used_Ratio' ], group_name='job_id',
                                keep=['component_id'],
                                xfrm_suffix="")
                xfrm.push(memUsedRatio)
                counts = [ len(res) ]
                _max = xfrm.max([ 'Mem_Used_Ratio' ], group_name='job_id',
                                keep=['component_id'],
                                xfrm_suffix="")
                res = res.concat(_max)
                counts.append(len(_max))
                i = -2
                mem_used = []
                jid = []
                cid = []
                while i < 3:
                    lim = mean[[0,0]] + (float(i) * std[[0,0]])
                    mem_used.append(lim)
                    if i == 0:
                        _count = []
                    elif i < 0:
                        _count = memUsedRatio < ('Mem_Used_Ratio', lim)
                    else:
                        _count = memUsedRatio > ('Mem_Used_Ratio', lim)
                    counts.append(len(_count))
                    del(_count)
                    jid.append(job_id)
                    cid.append(0)
                    i += 1
                _res = DataSet()
                _res.append_array(len(mem_used), 'Mem_Used_Ratio', mem_used)
                _res.append_array(5, 'job_id', jid)
                _res.append_array(5, 'component_id', cid)
                res = res.concat(_res)
                res.append_array(7, "Analysis", ["Min", "Max", "Std -2", "Std -1", "Mean", "Std +1", "Std +2" ])
                res.append_array(7, 'Count', counts)
                res = f.fmt_table(res)
                return res

            if low_not_high:
                # select the data from memUsedRatio where the values in the series
                # 'Mem_Used_Ratio' are less than limit
                limit = mean[[0,0]] - (float(threshold) * std[[0,0]])
                res = memUsedRatio < ('Mem_Used_Ratio', limit)
            else:
                # select the data from memUsedRatio where the values in the series
                # 'Mem_Used_Ratio' are greater than limit
                limit = mean[[0,0]] + (float(threshold) * std[[0,0]])
                res = memUsedRatio > ('Mem_Used_Ratio', limit)

            xfrm.push(res)
            if idle:
                if low_not_high:
                    xfrm.min([ 'Mem_Used_Ratio' ], group_name='component_id',
                             keep=['timestamp', 'job_id', 'component_id'])
                else:
                    xfrm.max([ 'Mem_Used_Ratio' ], group_name='component_id',
                             keep=['timestamp', 'job_id', 'component_id'])
            else:
                if low_not_high:
                    xfrm.min([ 'Mem_Used_Ratio' ], group_name='job_id',
                             keep=['timestamp', 'job_id', 'component_id'])
                else:
                    xfrm.max([ 'Mem_Used_Ratio' ], group_name='job_id',
                             keep=['timestamp', 'job_id', 'component_id'])
            res = xfrm.pop()
            # res.show()
            res = f.fmt_table(res)
            return res
        except Exception as e:
            a, b, c = sys.exc_info()
            log.write(str(e)+" "+str(c.tb_lineno))
            return None

    def lustreMetricSecond(self, metrics, start, end):

        src = SosDataSource()
        src.config(cont=self.cont)
        try:
            where_ = [ [ 'timestamp', Sos.COND_GE, start ], 
                       [ 'timestamp', Sos.COND_LE, end ] ]
            src.select(metrics + ['job_id'],
                       from_ = [ 'Lustre_Client' ],
                       where = where_,
                       order_by = 'time_job_comp'
                )
            xfrm = Transform(src, None)
            res = xfrm.begin()
            if res is None:
                return None
            while res is not None:
                res = xfrm.next()
                if res is not None:
                    xfrm.concat()

            sum_ = xfrm.sum(metrics, group_name='job_id',
                          keep=['job_id'])
            metric_sum = np.zeros(sum_.get_series_size())
            for m in sum_.series[1:]:
                metric_sum += sum_.array(m)
            delta_ts = end - start
            metric_sum = metric_sum / delta_ts
            return metric_sum, sum_
        except Exception as e:
            a, b, c = sys.exc_info()
            log.write('metricPsecond '+str(e)+' '+str(c.tb_lineno))
            return None, None

    def lustreMetaData(self, metrics, start, end, threshold):
        try:
            iops, sum_ = self.lustreMetricSecond(metrics, start, end)
            i = 0
            ret_iops = []
            ret_jobs = []
            while i < threshold:
                index, val = max(enumerate(iops), key=operator.itemgetter(1))
                jids = sum_.array('job_id')
                if sum_.array('job_id')[index] == 0:
                    iops = np.delete(iops, index)
                    jids = np.delete(jids, index)
                    continue
                ret_iops.append(int(val))
                ret_jobs.append(sum_.array('job_id')[index])
                jids = np.delete(jids, index)
                iops = np.delete(iops, index)
                i += 1
            res = DataSet()
            res.append_array(len(ret_jobs), 'job_id', ret_jobs)
            res.append_array(len(ret_iops), 'IO/s', ret_iops)
            f = grafana.DataSetFormatter()
            res = f.fmt_table(res)
            return res
        except Exception as e:
            a, b, c = sys.exc_info()
            log.write('lustreMetaData '+str(e)+' '+str(c.tb_lineno))
            return None

    def lustreData(self, metrics, start, end, threshold):
        try:
            bps, sum_ = self.lustreMetricSecond(metrics, start, end)
            i = 0
            ret_bps = []
            ret_jobs = []
            while i < threshold:
                index, val = max(enumerate(bps), key=operator.itemgetter(1))
                jids = sum_.array('job_id')
                if sum_.array('job_id')[index] == 0:
                    bps = np.delete(bps, index)
                    jids = np.delete(jids, index)
                    continue
                ret_bps.append(int(val))
                ret_jobs.append(sum_.array('job_id')[index])
                jids = np.delete(jids, index)
                bps = np.delete(bps, index)
                i += 1
            res = DataSet()
            res.append_array(len(ret_jobs), 'job_id', ret_jobs)
            res.append_array(len(ret_bps), 'Bps', ret_bps)
            f = grafana.DataSetFormatter()
            res = f.fmt_table(res)
            return res
        except Exception as e:
            a, b, c = sys.exc_info()
            log.write('lustreData '+str(e)+' '+str(c.tb_lineno))
            return None
