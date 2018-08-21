import Component from '@ember/component';
import { set } from '@ember/object';

import { IDatasetBreadcrumb } from 'wherehows-web/utils/entities/bake-urn-breadcrumbs';

export default class DatasetsUrnBreadcrumbsCrumb extends Component {
  tagName = 'li';

  classNames = ['nacho-breadcrumbs__crumb'];

  /**
   * Maximum number of characters to render from crumb text
   * @type {number}
   * @memberof {DatasetsUrnBreadcrumbsCrumb}
   */
  maxCrumbCharLength: number;

  /**
   * The instance of IDatasetBreadcrumb containing navigation and display properties to render
   * @type {IDatasetBreadcrumb}
   * @memberof {DatasetsUrnBreadcrumbsCrumb}
   */
  crumb: IDatasetBreadcrumb;

  constructor() {
    super(...arguments);

    // defaults
    typeof this.maxCrumbCharLength === 'number' || set(this, 'maxCrumbCharLength', 12);
  }
}
