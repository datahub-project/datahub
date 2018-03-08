import Component from '@ember/component';
import { get, set } from '@ember/object';
import { task } from 'ember-concurrency';
import { DatasetPlatform, nodeToQueryParams } from 'wherehows-web/constants';
import { IBrowserRouteParams } from 'wherehows-web/routes/browse/entity';
import { readPlatforms } from 'wherehows-web/utils/api/platforms/platform';
import { arrayMap } from 'wherehows-web/utils/array';
import { IReadDatasetsOptionBag } from 'wherehows-web/typings/api/datasets/dataset';
import { isDatasetIdentifier, sanitizePlatformNodeString } from 'wherehows-web/utils/validators/platform';
import { buildLiUrn } from 'wherehows-web/utils/validators/urn';

/**
 * Describes a node with parameters used by dynamic-link component to create links to items listed in the rail
 * @interface IRailNode
 */
interface IRailNode {
  title: string;
  text: string;
  route: 'browse.entity' | 'datasets.dataset';
  model: IBrowserRouteParams['entity'];
  queryParams?: Partial<IReadDatasetsOptionBag>;
}

/**
 * Given a platform and entity, returns a closure function that maps each node to a
 * list of IRailNode
 * @param {DatasetPlatform} platform
 * @param {IBrowserRouteParams.entity} entity
 * @returns {(array: string[]) => IRailNode[]}
 */
export const mapNodeToRoute = (
  platform: DatasetPlatform,
  entity: IBrowserRouteParams['entity']
): ((array: string[]) => IRailNode[]) =>
  arrayMap((node: string): IRailNode => {
    //FIXME: measure perf, and see if sanitize step can be performed conditionally for list, in a Schwartzian transform instead
    const sanitizedString = sanitizePlatformNodeString(node);
    const baseProps = {
      title: sanitizedString,
      text: sanitizedString
    };

    // If node is a dataset identifier, then create link to jump to dataset
    if (isDatasetIdentifier(node)) {
      return {
        ...baseProps,
        route: 'datasets.dataset',
        model: buildLiUrn(platform, node)
      };
    }

    return {
      ...baseProps,
      route: 'browse.entity',
      model: entity,
      queryParams: nodeToQueryParams({ platform, node })
    };
  });

export default class BrowserRail extends Component {
  /**
   * Passed in parameters containing route or query parameters values to be used in request
   * @type {IBrowserRouteParams}
   * @memberof BrowserRail
   */
  params: IBrowserRouteParams;

  /**
   * Maintains a list the nodes platforms or prefixes available in the selected entity
   * @type {Array<IRailNode>}
   * @memberof BrowserRail
   */
  nodes: Array<IRailNode> = [];

  didUpdateAttrs() {
    this._super(...arguments);
    get(this, 'getNodesTask').perform();
  }

  didInsertElement() {
    this._super(...arguments);
    get(this, 'getNodesTask').perform();
  }

  /**
   * Gets the nodes: platforms, or prefixes for the selected entity
   * @type {TaskProperty<Promise<string[]>> & {perform: (a?: {} | undefined) => TaskInstance<Promise<string[]>>}}
   * @memberof BrowserRail
   */
  getNodesTask = task(function*(this: BrowserRail): IterableIterator<Promise<Array<string>>> {
    const { prefix, platform, entity } = get(this, 'params');
    const nodes: Array<IRailNode> = mapNodeToRoute(<DatasetPlatform>platform, entity)(
      yield readPlatforms({ platform, prefix })
    );

    set(this, 'nodes', nodes);
  }).drop();
}
