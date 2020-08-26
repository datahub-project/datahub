import Component from '@ember/component';
import { tagName, layout } from '@ember-decorators/component';
import { computed } from '@ember/object';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { IDynamicLinkNode } from '@datahub/utils/types/vendor/dynamic-link';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/browser/search-within-hierarchy';

/**
 * Indicates the total number of entities within a category and on user interaction
 * performs an advanced search query for entities within the related category
 * @export
 * @class BrowserSearchWithinHierarchy
 * @extends {Component}
 */
@layout(template)
@tagName('')
export default class BrowserSearchWithinHierarchy extends Component {
  /**
   * The identifier for a DataModelEntity
   */
  entityType?: DataModelEntity['displayName'];

  /**
   * Count of entities within the related browse hierarchy
   */
  count = 0;

  /**
   * Browse hierarchy currently being viewed, represented by the list of segments
   */
  segments: Array<string> = [];

  /**
   * Attribute to reference component element
   */
  readonly searchHierarchy = '';

  /**
   * Call To Action for the component element,
   * e.g. `View 20 features`
   * @readonly
   */
  @computed('count', 'entityType')
  get cta(): string {
    const { count, entityType } = this;
    return count && entityType ? `View ${count} ${entityType}` : '';
  }

  /**
   * get link query params
   */
  @computed('entityType', 'segments', 'cta')
  get link(): IDynamicLinkNode<Array<string>, 'browsesearch.entity', { page: number; path: string }> {
    const { entityType = '', segments, cta } = this;
    return {
      text: cta,
      title: cta,
      model: [entityType],
      route: 'browsesearch.entity',
      queryParams: {
        page: 1,
        path: segments.join('/')
      }
    };
  }
}
