Overview
========

This is a django project that provides SOS support for Grafana. It relies on sosdb-ui
DSOS, and apache.

Installation
============
Install Dependencies:
    sosdb-ui
    SOS
    dsosds (Grafana datasource for DSOS)
    Grafana

Install package from source
===========================
Sodb-grafana must be built with the same prefix as sosdb-ui

./autogen.sh
mkdir build
cd build
../configure --prefix=<default /var/www/ovis_web_svcs>
make
make install

Grafana Configuration
======================
Configure grafana for your deployment.
Ensure to include the line "allow_loading_unsigned_plugins = dsosds" in the grafana configuration file.
If excluded, the dsosds plugin will not be loaded into grafana.

## SOS JSON Datasource
========================
Implements the following urls:

 * `/` should return 200 ok. Used for "Test connection" on the datasource config page.
 * `/search` used by the find metric options on the query tab in panels.
 * `/query` returns metrics based on input.
 * `/annotations` is not currently supported by sos_web_svcs.

### Query API
Format
	- table or time_series
	- some Query Types will only return table or time_series
Container
	- name of container to query from
Schema
	- schema to query from
Job ID
	- job_id to query data from
	- may be left blank or 0
Comp ID
	- component_id to query data from
Metric
	- a metric from the specified schema to query for
Query Type:
	metrics
		- returns metric datapoints over time
	analysis
		- informs back end to look for "analysis" parameter
		- input name of numsos analysis module in "Analysis" parameter to use
	papi_rank_table
		- displays rank information for a given job
		- requires job_id
	papi_job_summary
		 - displays job summary info for a given PAPI job
		- requires job_id
	job_table
		- returns list of recent jobs and relevant info
	like_jobs
		- returns jobs related to job_id provided. requires kokkos_app schema
		- requires job_id
	papi_timeseries
		- returns time_series data for papi metrics
	
Analysis:
	- text input by user
	- user defined python analytics module. pre-packaged modules currently include:
	rankMemByJob
		- gets the high or low memory thresholds across jobs in a given time range
		Extra Parameters:
			- threshold=<threshold>
				-> set to a positive integer to get top N jobs with memory usage
				-> set to a negative integer to get bottom N jobs with memory usage
			- summary
				-> specifying summary will return a summary across jobs in time
				   range
				-> returns standard deviation -2 > x > 2, as well as high and
				   low job
			- Inlcude "idle" parameter before "threshold", e.g. "idle&threshold=10"
				- gets the high or low memory thresholds across components
				  not running jobs
	compMinMeanMax
		- returns the min, mean, and max datapoints for a job's particular metric across components
		- only one metric may be specified
	lustreData
		- returns sum of metrics provided per second ( Bytes per second )
		Extra Parameters:
			- threshold=<threshold>
				
			- meta
				- must be included before "threshold" in query parameters e.g.
					"meta&threshold=10"
				- returns sum of metrics provided per second
				  ( Inputs/Outputs per second )
				
Extra Parameters: Extra arguments used when analysis other than metric is selected



Example response
``` javascript
[
  {
    "target":"current_freemem", // The field being queried for 
    "datapoints":[
      [622,1450754160000],  // Metric value as a float , unixtimestamp in milliseconds
      [365,1450754220000]
    ]   
  },  
  {
    "target":"nrx_RDMA",
    "datapoints":[
      [861,1450754160000],
      [767,1450754220000]
    ]   
  }
]
```

```
Access-Control-Allow-Headers:accept, content-type
Access-Control-Allow-Methods:POST
Access-Control-Allow-Origin:*
```

### Search API
These template variables can be defined in the dashboard settings under "variables".
Example request
        The first parameter in the target is the desired data
                SCHEMA:
                        Syntax: query=schema&container=<cont_name>
                INDEX:
                        Syntax: query=index&container=<cont_name>&schema=<schema_name>
                METRICS:
                        Syntax: query=metrics&container=<cont_name>&schema=<schema_name>
                COMPONENTS:
                        Syntax: query=components&container=<cont_name>&schema=<schema_name>
                JOBS:
                        Syntax: query=jobs&container=<cont_name>&schema=<schema_name>
```

The search api can either return an array or map.

Example array response
``` javascript
{ current_freemem : 'current_freemem', nrx_RDMA : 'nrx_RDMA' }
```

Annotations
============

Query format is as such:
	container=<cont_name>&job_id=<job_id>&comp_id=<comp_id>


