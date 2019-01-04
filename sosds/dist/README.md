## SOS JSON Datasource

Implements the following urls:

 * `/` should return 200 ok. Used for "Test connection" on the datasource config page.
 * `/search` used by the find metric options on the query tab in panels.
 * `/query` returns metrics based on input.
 * `/annotations` is not currently supported by sos_web_svcs.

### Query API

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
	The first parameter in teh target is the desired data
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

### Changelog

Copy the data source you want to /public/app/plugins/datasource/. Then restart grafana-server. The new data source should now be available in the data source type dropdown in the Add Data Source View.
