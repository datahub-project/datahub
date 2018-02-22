import Component from '@ember/component';
import { computed, get } from '@ember/object';
import ComputedProperty from '@ember/object/computed';
import { bakeUrnBreadcrumbs } from 'wherehows-web/utils/entities';
import { IDatasetBreadcrumb } from 'wherehows-web/utils/entities/bake-urn-breadcrumbs';

export default class UrnBreadcrumbs extends Component {
  tagName = 'ul';

  classNames = ['nacho-breadcrumbs'];

  /**
   * Urn for the dataset to be represented in the breadcrumbs
   * @type {string}
   * @memberof UrnBreadcrumbs
   */
  urn: string;

  /**
   * Builds the breadcrumbs for the dataset with the related urn
   * @type {ComputedProperty<Array<IDatasetBreadcrumb>>}
   * @memberof UrnBreadcrumbs
   */
  breadcrumbs: ComputedProperty<Array<IDatasetBreadcrumb>> = computed('urn', function(
    this: UrnBreadcrumbs
  ): Array<IDatasetBreadcrumb> {
    return bakeUrnBreadcrumbs(get(this, 'urn'));
  });
}
