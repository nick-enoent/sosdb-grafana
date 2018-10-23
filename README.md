Overview
========

This is a django project that provides SOS support for Grafana. It relies on sosdb-ui
SOS, and apache.

Installation
============
Install Dependencies:
    sosdb-ui
    SOS
    sosds (Grafana datasource for SOS)
    Grafana

Templates
============
Query format is as such:
	container&schema&search_type
	
	Where:
		container
			name of the container to query
			If only container is defined, the template query will return schema names
		schema
			string of schema in container to query
			If only container and schema are defined, the template query will return
				metrics by default
		search_type
			If search_type is "metrics", the template query will return metrics
			If search_type is "index", the template query will return index attributes

Annotations
============

Query format is as such:
	container=<cont_name>&job_id=<job_id>&comp_id=<comp_id>
