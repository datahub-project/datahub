import Component from '@ember/component';
import { set } from '@ember/object';
import { bakeUrnBreadcrumbs } from 'wherehows-web/utils/entities';
import { IDatasetBreadcrumb } from 'wherehows-web/utils/entities/bake-urn-breadcrumbs';
import { computed } from '@ember-decorators/object';

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
   * The maximum number of items / crumbs to show in the breadcrumb navigation trail
   * @type {number}
   * @memberof UrnBreadcrumbs
   */
  maxTrailLength: number;

  constructor() {
    super(...arguments);

    // defaults
    typeof this.maxTrailLength === 'number' || set(this, 'maxTrailLength', 8);
  }
  /**
   * Builds the breadcrumbs for the dataset with the related urn
   * @readonly
   * @type {ComputedProperty<Array<IDatasetBreadcrumb>>}
   * @memberof UrnBreadcrumbs
   */
  @computed('urn')
  get breadcrumbs(): Array<IDatasetBreadcrumb> {
    return bakeUrnBreadcrumbs(this.urn);
  }

  /**
   * Builds a hash of breadcrumb parts affix parts
   * When items / crumbs in trail exceed this.maxTrailLength, split into hash with the ffg:
   *  prefix - containing prepend list of crumbs
   *  suffix - containing append list of crumbs
   *  infix - middle list of crumbs, which may not be rendered
   * @readonly
   * @type {ComputedProperty<(Record<'prefix' | 'infix' | 'suffix', Array<IDatasetBreadcrumb>>)>}
   * @memberof UrnBreadcrumbs
   */
  @computed('breadcrumbs')
  get affix(): Record<'prefix' | 'infix' | 'suffix', Array<IDatasetBreadcrumb>> {
    const { breadcrumbs, maxTrailLength } = this;
    const trailLength = breadcrumbs.length;
    // default affix, will contain all elements as prefix property
    const affix = { prefix: breadcrumbs, infix: [], suffix: [] };

    // number of items in trail exceeds allowed maximum
    if (trailLength > maxTrailLength) {
      const breakAt = Math.floor(maxTrailLength / 2);
      const prefix = breadcrumbs.slice(0, breakAt);
      const suffix = breadcrumbs.slice(-breakAt);
      const infix = breadcrumbs.slice(breakAt, -breakAt);

      return { prefix, suffix, infix };
    }

    return affix;
  }
}
