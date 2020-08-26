import Component from '@ember/component';
import { tagName, classNames, layout } from '@ember-decorators/component';
import { IBreadcrumb } from '@datahub/shared/components/browser/entity-breadcrumbs';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/browser/entity-breadcrumbs/crumb';

@layout(template)
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
