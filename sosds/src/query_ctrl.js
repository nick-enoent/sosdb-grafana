import {QueryCtrl} from 'app/plugins/sdk';
import './css/query-editor.css!'

export class SosDatasourceQueryCtrl extends QueryCtrl {
    constructor($scope, $injector, uiSegmentSrv)  {
        super($scope, $injector);

        this.scope = $scope;

        this.lastQueryMeta = null;
        this.lastQueryError = null;

        this.uiSegmentSrv = uiSegmentSrv;

        this.target.target = this.target.target || 'select metric';
        this.target.type = this.target.type || 'timeserie';
        this.target.container = this.target.container;
        this.target.schema = this.target.schema;
        this.target.job_id = this.target.job_id || 0;
        this.target.comp_id = this.target.comp_id || 0;
        this.target.user_name = this.target.user_name;
        this.target.prdcr_name = this.target.prdcr_name;
        this.target.query_type = this.target.query_type || 'metrics';
        this.target.analysis = this.target.analysis;
        this.target.format = this.target.format || 'time_series';
        this.target.extra_params = this.target.extra_params;

        this.panelCtrl.events.on('data-received', this.onDataReceived.bind(this), $scope);
        this.panelCtrl.events.on('data-error', this.onDataError.bind(this), $scope);
    }

    onDataReceived(dataList) {
	this.lastQueryMeta = null;
	this.lastQueryError = null;

	let anySeriesFromQuery = _.find(dataList, { refId: this.target.refId });
	if (anySeriesFromQuery) {
	    this.lastQueryMeta = anySeriesFromQuery.meta;
	}
    }

    onDataError(err) {
	if (err.data && err.data.results) {
	    let queryRes = err.data.results[this.target.refId];
	    if (queryRes) {
		this.lastQueryMeta = queryRes.meta;
		this.lastQueryError = queryRes.error;
	    }
	}
    }
    /*
      getMetrics() {
      this.cont_schema = this.target.container +'&'+this.target.schema+'&metric_attrs'
      return this.datasource.metricFindQuery(this.cont_schema)
      .then(this.uiSegmentSrv.transformToSegments(false));
      }

      getIdxAttrs() {
      this.cont_schema = this.target.container+'&'+this.target.schema+'&idx_attrs'
      return this.datasource.metricFindQuery(this.cont_schema)
      .then(this.uiSegmentSrv.transformToSegments(false));
      }

      getOptions() {
      return this.datasource.metricFindQuery(this.target)
      .then(this.uiSegmentSrv.transformToSegments(false));
      // Options have to be transformed by uiSegmentSrv to be usable by metric-segment-model directive
      }
    */

    toggleEditorMode() {
	this.target.rawQuery = !this.target.rawQuery;
    }

    onChangeInternal() {
	this.panelCtrl.refresh(); // Asks the panel to refresh data.
    }
}

SosDatasourceQueryCtrl.templateUrl = 'partials/query.editor.html';

