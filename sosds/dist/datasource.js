"use strict";

System.register(["lodash"], function (_export, _context) {
	"use strict";

	var _, _createClass, SosDatasource;

	function _classCallCheck(instance, Constructor) {
		if (!(instance instanceof Constructor)) {
			throw new TypeError("Cannot call a class as a function");
		}
	}

	return {
		setters: [function (_lodash) {
			_ = _lodash.default;
		}],
		execute: function () {
			_createClass = function () {
				function defineProperties(target, props) {
					for (var i = 0; i < props.length; i++) {
						var descriptor = props[i];
						descriptor.enumerable = descriptor.enumerable || false;
						descriptor.configurable = true;
						if ("value" in descriptor) descriptor.writable = true;
						Object.defineProperty(target, descriptor.key, descriptor);
					}
				}

				return function (Constructor, protoProps, staticProps) {
					if (protoProps) defineProperties(Constructor.prototype, protoProps);
					if (staticProps) defineProperties(Constructor, staticProps);
					return Constructor;
				};
			}();

			_export("SosDatasource", SosDatasource = function () {
				function SosDatasource(instanceSettings, $q, backendSrv, templateSrv) {
					_classCallCheck(this, SosDatasource);

					this.type = instanceSettings.type;
					this.url = instanceSettings.url;
					this.name = instanceSettings.name;
					this.q = $q;
					this.backendSrv = backendSrv;
					this.templateSrv = templateSrv;
				}

				_createClass(SosDatasource, [{
					key: "query",
					value: function query(options) {
						var query = this.buildQueryParameters(options);
						query.targets = query.targets.filter(function (t) {
							return !t.hide;
						});

						console.log("query: " + options);

						if (query.targets.length <= 0) {
							return this.q.when({ data: [] });
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
				}, {
					key: "testDatasource",
					value: function testDatasource() {
						return this.backendSrv.datasourceRequest({
							url: this.url + '/',
							method: 'GET'
						}).then(function (response) {
							if (response.status === 200) {
								return { status: "success", message: 'SOS Connection OK', title: "Success" };
							}
						}).catch(function (err) {
							console.log(err);
							if (err.data && err.data.message) {
								return { status: 'error', message: err.data.message };
							} else {
								return { status: 'error', message: err.status };
							}
						});
					}
				}, {
					key: "annotationQuery",
					value: function annotationQuery(options) {
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

						console.log("annotationQuery");

						return this.backendSrv.datasourceRequest({
							url: this.url + '/annotations',
							method: 'POST',
							data: annotationQuery
						}).then(function (result) {
							return result.data;
						});
					}
				}, {
					key: "metricFindQuery",
					value: function metricFindQuery(options, optionalOptions) {
						var target = typeof options === "string" ? options : options.target;
						var interpolated = {
							target: this.templateSrv.replace(target, null, 'regex')
						};
						console.log("metricFindQuery: " + options + "\n" + optionalOptions);
						return this.backendSrv.datasourceRequest({
							url: this.url + '/search',
							data: interpolated,
							method: 'POST',
							headers: { 'Content-Type': 'application/json' }
						}).then(this.mapToTextValue);
					}
				}, {
					key: "mapToTextValue",
					value: function mapToTextValue(result) {
						return _.map(result.data, function (d, i) {
							if (d && d.text && d.value) {
								return { text: d.text, value: d.value };
							}
							return { text: d, value: i };
						});
					}
				}, {
					key: "buildQueryParameters",
					value: function buildQueryParameters(options) {
						var _this = this;

						//remove placeholder targets
						var targets = _.map(options.targets, function (target) {
							return {
								target: _this.templateSrv.replace(target.target),
								container: _this.templateSrv.replace(target.container),
								schema: _this.templateSrv.replace(target.schema),
								job_id: _this.templateSrv.replace(target.job_id),
								comp_id: _this.templateSrv.replace(target.comp_id),
								user_name: _this.templateSrv.replace(target.user_name),
								query_type: _this.templateSrv.replace(target.query_type) || 'metrics',
								format: _this.templateSrv.replace(target.format) || 'time_series',
								analysis: _this.templateSrv.replace(target.analysis),
								extra_params: _this.templateSrv.replace(target.extra_params),
								refId: target.refId,
								hide: target.hide,
								type: target.type || 'timeserie'
							};
						});

						options.targets = targets;

						return options;
					}
				}]);

				return SosDatasource;
			}());

			_export("SosDatasource", SosDatasource);
		}
	};
});
//# sourceMappingURL=datasource.js.map
