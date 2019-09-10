import _ from "lodash";

export class SosDatasource {

    constructor(instanceSettings, $q, backendSrv, templateSrv) {
        this.type = instanceSettings.type;
        this.url = instanceSettings.url;
        this.name = instanceSettings.name;
        this.q = $q;
        this.backendSrv = backendSrv;
        this.templateSrv = templateSrv;
    }

    query(options) {
        var query = this.buildQueryParameters(options);
        query.targets = query.targets.filter(t => !t.hide);

        console.log("query: "+options)

        if (query.targets.length <= 0) {
            return this.q.when({data: []});
        }

        return this.backendSrv.datasourceRequest({
            url: this.url + '/query',
            method: 'POST',
            data: options, // query,
	    /*
            data: {
                from: options.range.from.valueOf().toString(),
                to: options.range.to.valueOf().toString(),
		queries: query,
	    },
	    */
	    headers: { 'Content-Type': 'application/json' }
	});
    }

    testDatasource() {
	return this.backendSrv.datasourceRequest({
	    url: this.url + '/',
	    method: 'GET'
	}).then(response => {
	    if (response.status === 200) {
		return { status: "success", message: 'SOS Connection OK', title: "Success" };
	    }
	}).catch(err => {
	    console.log(err);
	    if (err.data && err.data.message) {
		return { status: 'error', message: err.data.message };
	    } else {
		return { status: 'error', message: err.status  };
	    }
	});
    }

    annotationQuery(options) {
	var query = this.templateSrv.replace(options.annotation.query, {}, 'glob');
	var annotationQuery = {
	    range: options.range,
	    annotation: {
		name: options.annotation.name,
		datasource: options.annotation.datasource,
		enable: options.annotation.enable,
		iconColor: options.annotation.iconColor,
		query: query
	    },
	    rangeRaw: options.rangeRaw
	};

	console.log("annotationQuery")

	return this.backendSrv.datasourceRequest({
	    url: this.url + '/annotations',
	    method: 'POST',
	    data: annotationQuery
	}).then(result => {
	    return result.data;
	});
    }

    metricFindQuery(options, optionalOptions) {
	var target = typeof (options) === "string" ? options : options.target;
	var interpolated = {
            target: this.templateSrv.replace(target, null, 'regex')
	};
	console.log("metricFindQuery: " + options + "\n" + optionalOptions)
	return this.backendSrv.datasourceRequest({
	    url: this.url + '/search',
	    data: interpolated,
	    method: 'POST',
	    headers: { 'Content-Type': 'application/json' }
	}).then(this.mapToTextValue);
    }

    mapToTextValue(result) {
	return _.map(result.data, (d, i) => {
	    if (d && d.text && d.value) {
		return { text: d.text, value: d.value };
	    }
	    return { text: d, value: i };
	});
    }

    buildQueryParameters(options) {
	//remove placeholder targets
	var targets = _.map(options.targets, target => {
	    return {
		target: this.templateSrv.replace(target.target),
		container: this.templateSrv.replace(target.container),
		schema: this.templateSrv.replace(target.schema),
		index: this.templateSrv.replace(target.index),
		job_id: this.templateSrv.replace(target.job_id),
		comp_id: this.templateSrv.replace(target.comp_id),
		query_type: this.templateSrv.replace(target.query_type) || 'metrics',
		format: this.templateSrv.replace(target.format),
                analysis: this.templateSrv.replace(target.analysis),
                extra_params: this.templateSrv.replace(target.extra_params),
		refId: target.refId,
		hide: target.hide,
		type: target.type || 'timeserie'
	    };
	});

	options.targets = targets;

	return options;
    }
}
