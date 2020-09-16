import Component from '@ember/component';
import { classNames, layout } from '@ember-decorators/component';
import { computed } from '@ember/object';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/browser/entity-breadcrumbs';

/**
 * Describes the interface for properties in a breadcrumb node
 * @export
 * @interface IBreadcrumb
 */
export interface IBreadcrumb {
  // Display value for the breadcrumb
  label: string;
  // The query parameter value for the prefix string which maps to the remaining segments in the entity hierarchy
  path: string;
}

/**
 * Aliases a list of breadcrumb objects
 */
type Trail = Array<IBreadcrumb>;

/**
 * Parses a list of segments with the first segment treated as corresponding to the category dynamic segment
 * in the browse.entity route
 * @param {...Array<string>} entityHierarchySegments
 * @returns {Trail}
 */
const bakeBreadcrumbs = (...entityHierarchySegments: Array<string>): Trail => {
  return entityHierarchySegments.reduce((breadcrumbs: Trail, segment: string, index: number): Trail => {
    let path = '';

    // static value for delimiting prefix segments
    const prefixSeparator = '/';
    const previousPrefix = breadcrumbs[index - 1] ? breadcrumbs[index - 1].path : '';
    // Ensure we don't join on undefined or empty strings, this prevents trailing or leading slashes
    path = [previousPrefix, segment].filter(Boolean).join(prefixSeparator);

    return [
      ...breadcrumbs,
      {
        path,
        label: segment
      }
    ];
  }, []);
};

/**
 * Defines EntityBreadcrumbs properties and  methods to generate the breadcrumbs component
 * @export
 * @class BrowserEntityBreadcrumbs
 * @extends {Component}
 */
@layout(template)
@classNames('nacho-breadcrumbs-container')
export default class BrowserEntityBreadcrumbs extends Component {
  /**
   * Maximum number of crumbs to show
   * @type {number}
   * @memberof BrowserEntityBreadcrumbs
   */
  maxTrailLength = 8;

  /**
   * External attribute for the category segment value in the url for browse.entity.category
   * @type {string}
   * @memberof BrowserEntityBreadcrumbs
   */
  categoryPath = '';

  /**
   * List of entity category segments used to generate the breadcrumbs, this is typically
   * generated from the prefix and category values
   * @type {Array<string>}
   * @memberof BrowserEntityBreadcrumbs
   */
  segments: Array<string> = [];

  /**
   * Lists the IBreadcrumb properties to build the breadcrumbs
   * @readonly
   * @type {Trail}
   * @memberof BrowserEntityBreadcrumbs
   */
  @computed('segments.[]')
  get breadcrumbs(): Trail {
    return bakeBreadcrumbs(...(this.segments || []));
  }

  /**
   * Depending on the maxTrailLength value chunks the breadcrumbs into visual sections for ui rendering
   * if an infix and suffix list exists, these are rendered independently of the prefix
   * @readonly
   * @type {(Record<'prefix' | 'infix' | 'suffix', Trail>)}
   * @memberof BrowserEntityBreadcrumbs
   */
  @computed('breadcrumbs.[]')
  get affix(): Record<'prefix' | 'infix' | 'suffix', Trail> {
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
