import Component from '@ember/component';
import { tagName, classNames } from '@ember-decorators/component';
import { IBreadcrumb } from 'wherehows-web/components/browser/entity-breadcrumbs';

@tagName('li')
@classNames('nacho-breadcrumbs__crumb')
export default class BrowserEntityBreadcrumbsCrumb extends Component {
  /**
   * The IBreadcrumb properties to be rendered for this component
   * @type {IBreadcrumb}
   * @memberof BrowserEntityBreadcrumbsCrumb
   */
  crumb?: IBreadcrumb;
}
