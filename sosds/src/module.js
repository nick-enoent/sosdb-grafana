import {SosDatasource} from './datasource';
import {SosDatasourceQueryCtrl} from './query_ctrl';

class SosConfigCtrl {}
SosConfigCtrl.templateUrl = 'partials/config.html';

class SosQueryOptionsCtrl {}
SosQueryOptionsCtrl.templateUrl = 'partials/query.options.html';

class SosAnnotationsQueryCtrl {}
SosAnnotationsQueryCtrl.templateUrl = 'partials/annotations.editor.html'

export {
  SosDatasource as Datasource,
  SosDatasourceQueryCtrl as QueryCtrl,
  SosConfigCtrl as ConfigCtrl,
  SosQueryOptionsCtrl as QueryOptionsCtrl,
  SosAnnotationsQueryCtrl as AnnotationsQueryCtrl
};
