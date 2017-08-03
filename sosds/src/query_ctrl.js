import {QueryCtrl} from 'app/plugins/sdk';
import './css/query-editor.css!'

export class SosDatasourceQueryCtrl extends QueryCtrl {
  constructor($scope, $injector, uiSegmentSrv)  {
    super($scope, $injector);

    this.scope = $scope;
    this.uiSegmentSrv = uiSegmentSrv;
    this.target.target = this.target.target || 'select metric';
    this.target.type = this.target.type || 'timeserie';
    this.target.container = this.target.container;
    this.target.schema = this.target.schema;
    this.target.index = this.target.index;
    this.target.comp_id = this.target.comp_id;
    this.target.query_type = this.target.query_type || 'metrics';
    
    if (!this.target.query_type) {
        this.target.query_type = 'metrics';
    }
  }

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

  toggleEditorMode() {
    this.target.rawQuery = !this.target.rawQuery;
  }

  onChangeInternal() {
    this.panelCtrl.refresh(); // Asks the panel to refresh data.
  }
}

SosDatasourceQueryCtrl.templateUrl = 'partials/query.editor.html';

