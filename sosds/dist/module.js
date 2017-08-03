'use strict';

System.register(['./datasource', './query_ctrl'], function (_export, _context) {
  "use strict";

  var SosDatasource, SosDatasourceQueryCtrl, SosConfigCtrl, SosQueryOptionsCtrl, SosAnnotationsQueryCtrl;

  function _classCallCheck(instance, Constructor) {
    if (!(instance instanceof Constructor)) {
      throw new TypeError("Cannot call a class as a function");
    }
  }

  return {
    setters: [function (_datasource) {
      SosDatasource = _datasource.SosDatasource;
    }, function (_query_ctrl) {
      SosDatasourceQueryCtrl = _query_ctrl.SosDatasourceQueryCtrl;
    }],
    execute: function () {
      _export('ConfigCtrl', SosConfigCtrl = function SosConfigCtrl() {
        _classCallCheck(this, SosConfigCtrl);
      });

      SosConfigCtrl.templateUrl = 'partials/config.html';

      _export('QueryOptionsCtrl', SosQueryOptionsCtrl = function SosQueryOptionsCtrl() {
        _classCallCheck(this, SosQueryOptionsCtrl);
      });

      SosQueryOptionsCtrl.templateUrl = 'partials/query.options.html';

      _export('AnnotationsQueryCtrl', SosAnnotationsQueryCtrl = function SosAnnotationsQueryCtrl() {
        _classCallCheck(this, SosAnnotationsQueryCtrl);
      });

      SosAnnotationsQueryCtrl.templateUrl = 'partials/annotations.editor.html';

      _export('Datasource', SosDatasource);

      _export('QueryCtrl', SosDatasourceQueryCtrl);

      _export('ConfigCtrl', SosConfigCtrl);

      _export('QueryOptionsCtrl', SosQueryOptionsCtrl);

      _export('AnnotationsQueryCtrl', SosAnnotationsQueryCtrl);
    }
  };
});
//# sourceMappingURL=module.js.map
