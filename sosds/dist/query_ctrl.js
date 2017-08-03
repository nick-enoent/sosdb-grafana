'use strict';

System.register(['app/plugins/sdk', './css/query-editor.css!'], function (_export, _context) {
  "use strict";

  var QueryCtrl, _createClass, SosDatasourceQueryCtrl;

  function _classCallCheck(instance, Constructor) {
    if (!(instance instanceof Constructor)) {
      throw new TypeError("Cannot call a class as a function");
    }
  }

  function _possibleConstructorReturn(self, call) {
    if (!self) {
      throw new ReferenceError("this hasn't been initialised - super() hasn't been called");
    }

    return call && (typeof call === "object" || typeof call === "function") ? call : self;
  }

  function _inherits(subClass, superClass) {
    if (typeof superClass !== "function" && superClass !== null) {
      throw new TypeError("Super expression must either be null or a function, not " + typeof superClass);
    }

    subClass.prototype = Object.create(superClass && superClass.prototype, {
      constructor: {
        value: subClass,
        enumerable: false,
        writable: true,
        configurable: true
      }
    });
    if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass;
  }

  return {
    setters: [function (_appPluginsSdk) {
      QueryCtrl = _appPluginsSdk.QueryCtrl;
    }, function (_cssQueryEditorCss) {}],
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

      _export('SosDatasourceQueryCtrl', SosDatasourceQueryCtrl = function (_QueryCtrl) {
        _inherits(SosDatasourceQueryCtrl, _QueryCtrl);

        function SosDatasourceQueryCtrl($scope, $injector, uiSegmentSrv) {
          _classCallCheck(this, SosDatasourceQueryCtrl);

          var _this = _possibleConstructorReturn(this, (SosDatasourceQueryCtrl.__proto__ || Object.getPrototypeOf(SosDatasourceQueryCtrl)).call(this, $scope, $injector));

          _this.scope = $scope;
          _this.uiSegmentSrv = uiSegmentSrv;
          _this.target.target = _this.target.target || 'select metric';
          _this.target.type = _this.target.type || 'timeserie';
          _this.target.container = _this.target.container;
          _this.target.schema = _this.target.schema;
          _this.target.index = _this.target.index;
          _this.target.comp_id = _this.target.comp_id;
          _this.target.query_type = _this.target.query_type || 'metrics';

          if (!_this.target.query_type) {
            _this.target.query_type = 'metrics';
          }
          return _this;
        }

        _createClass(SosDatasourceQueryCtrl, [{
          key: 'getMetrics',
          value: function getMetrics() {
            this.cont_schema = this.target.container + '&' + this.target.schema + '&metric_attrs';
            return this.datasource.metricFindQuery(this.cont_schema).then(this.uiSegmentSrv.transformToSegments(false));
          }
        }, {
          key: 'getIdxAttrs',
          value: function getIdxAttrs() {
            this.cont_schema = this.target.container + '&' + this.target.schema + '&idx_attrs';
            return this.datasource.metricFindQuery(this.cont_schema).then(this.uiSegmentSrv.transformToSegments(false));
          }
        }, {
          key: 'getOptions',
          value: function getOptions() {
            return this.datasource.metricFindQuery(this.target).then(this.uiSegmentSrv.transformToSegments(false));
            // Options have to be transformed by uiSegmentSrv to be usable by metric-segment-model directive
          }
        }, {
          key: 'toggleEditorMode',
          value: function toggleEditorMode() {
            this.target.rawQuery = !this.target.rawQuery;
          }
        }, {
          key: 'onChangeInternal',
          value: function onChangeInternal() {
            this.panelCtrl.refresh(); // Asks the panel to refresh data.
          }
        }]);

        return SosDatasourceQueryCtrl;
      }(QueryCtrl));

      _export('SosDatasourceQueryCtrl', SosDatasourceQueryCtrl);

      SosDatasourceQueryCtrl.templateUrl = 'partials/query.editor.html';
    }
  };
});
//# sourceMappingURL=query_ctrl.js.map
