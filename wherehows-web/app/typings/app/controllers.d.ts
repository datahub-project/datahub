import SearchController from 'wherehows-web/controllers/search';
import DatasetController from 'wherehows-web/controllers/datasets/dataset';
import DatasetComplianceController from 'wherehows-web/controllers/datasets/dataset/compliance';

declare module '@ember/controller' {
  // eslint-disable-next-line typescript/interface-name-prefix
  interface Registry {
    search: SearchController;
    'datasets.dataset': DatasetController;
    'datasets.dataset.compliance': DatasetComplianceController;
  }
}
