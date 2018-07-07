import Component from '@ember/component';
import { get, set, computed } from '@ember/object';
import { IBrowserRouteParams } from 'wherehows-web/routes/browse/entity';
import { task } from 'ember-concurrency';
import { isDatasetIdentifier, sanitizePlatformNodeString } from 'wherehows-web/utils/validators/platform';
import { buildLiUrn } from 'wherehows-web/utils/validators/urn';
import { DatasetPlatform, nodeToQueryParams } from 'wherehows-web/constants';
import { arrayMap } from 'wherehows-web/utils/array';
import { readPlatforms } from 'wherehows-web/utils/api/platforms/platform';
import { IDynamicLinkNode } from 'wherehows-web/typings/app/datasets/dynamic-link';

/**
 * Transforms a string reference to a data-system to a dynamic link object
 * curries a function that accepts the data-system name and builds a link node
 * @param {DatasetPlatform} platform
 * @param {IBrowserRouteParams.entity} entity
 * @return {(node: string) => IDynamicLinkNode}
 */
const transformDataSystemRefToLinkNode = (platform: DatasetPlatform, entity: IBrowserRouteParams['entity']) =>
  /**
   * Maps a data-system name to a dynamic link node using the platform and entity information provided
   * @param {string} node name of the data system
   * @return {IDynamicLinkNode}
   */
  (node: string): IDynamicLinkNode => {
    const sanitizedString = sanitizePlatformNodeString(node);
    const baseLinkNode = {
      title: sanitizedString,
      text: sanitizedString
    };

    return isDatasetIdentifier(node)
      ? {
          ...baseLinkNode,
          route: 'datasets.dataset',
          model: buildLiUrn(platform, node)
        }
      : {
          ...baseLinkNode,
          route: 'browse.entity',
          model: entity,
          queryParams: nodeToQueryParams({ platform, node })
        };
  };

/**
 * Creates a mapping function that accepts a list of data-system paths and transforms to a list of
 * link parameters
 * @param {DatasetPlatform} platform
 * @param {IBrowserRouteParams.entity} entity
 * @return {(array: Array<string>) => Array<IDynamicLinkNode>}
 */
const mapDataSystemsPathToLinkNode = (platform: DatasetPlatform, entity: IBrowserRouteParams['entity']) =>
  arrayMap(transformDataSystemRefToLinkNode(platform, entity));

export default class DataSystemsContainer extends Component {
  /** Route / url properties to redirect to data system
   * @type {IBrowserRouteParams}
   */
  params: IBrowserRouteParams;

  /**
   * Lists the data-system paths converted to a list of link properties
   * @type {Array<IDynamicLinkNode>}
   */
  nodes: Array<IDynamicLinkNode>;

  /**
   * Computes the urn for the data-system from the platform and prefix string
   * @type {ComputedProperty<string>}
   */
  urn = computed('params', function(this: DataSystemsContainer): string {
    const { platform, prefix } = get(this, 'params');
    return buildLiUrn(<DatasetPlatform>platform, prefix);
  });

  didUpdateAttrs() {
    this._super(...arguments);
    get(this, 'getDataSystemsNodesTask').perform();
  }

  didInsertElement() {
    this._super(...arguments);
    get(this, 'getDataSystemsNodesTask').perform();
  }

  /**
   * Task to request data-systems from the platform endpoint and transform to a list of link nodes
   * @type {(Task<Promise<Array<string>>, (a?: any) => TaskInstance<Promise<Array<string>>>>)}
   */
  getDataSystemsNodesTask = task(function*(this: DataSystemsContainer): IterableIterator<Promise<Array<string>>> {
    const { prefix, platform, entity } = get(this, 'params');
    const nodes = mapDataSystemsPathToLinkNode(<DatasetPlatform>platform, entity)(
      yield readPlatforms({ platform, prefix })
    );

    set(this, 'nodes', nodes);
  }).restartable();
}
