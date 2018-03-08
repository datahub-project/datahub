import Component from '@ember/component';
import { get, setProperties, computed, getProperties } from '@ember/object';
import { task, TaskInstance } from 'ember-concurrency';
import { IBrowserRouteParams } from 'wherehows-web/routes/browse/entity';
import { IDatasetsGetResponse, IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { readDatasets } from 'wherehows-web/utils/api/datasets/dataset';
import { action } from 'ember-decorators/object';
import ComputedProperty from '@ember/object/computed';

// Describes the index signature for strategy pattern in the getEntityDataTask
type IGetEntityTaskStrategy = { [K in IBrowserRouteParams['entity']]: TaskInstance<Promise<IDatasetsGetResponse>> };

// Describes the operations that can be performed on an entity list
type listOp = 'push' | 'set';

export default class BrowserViewport extends Component {
  /**
   * Passed in parameters containing route or query parameters values to be used in request
   * @type {IBrowserRouteParams}
   * @memberof BrowserViewport
   */
  params: IBrowserRouteParams;

  /**
   * Initial value for the entity being viewed
   * @memberof BrowserViewport
   */
  currentEntity: IBrowserRouteParams['entity'] = 'datasets';

  /**
   * Ember route for the entities being rendered
   * @type {string}
   * @memberof BrowserViewport
   */
  entityRoute = '';

  /**
   * List of entities to be rendered in view
   * @type {Array<IDatasetView>}
   * @memberof BrowserViewport
   */
  entities: Array<IDatasetView> = [];

  /**
   * The total number of entities in the segment
   * @type {number}
   */
  total: number = 0;

  /**
   * The number of entities to request at a time
   * @type {number}
   */
  count: number = 0;

  /**
   * The position index in the list to begin requesting data from
   * @type {number}
   */
  start: number = 0;

  didUpdateAttrs() {
    this._super(...arguments);
    get(this, 'getEntityDataTask').perform();
  }

  didInsertElement() {
    this._super(...arguments);
    get(this, 'getEntityDataTask').perform();
  }

  /**
   * Async request a list of datasets with a variable start point
   * @type {TaskProperty<Promise<IDatasetsGetResponse>> & {perform: (a1: "push" | "set" | undefined, a2: number) => TaskInstance<Promise<IDatasetsGetResponse>>}}
   * @memberof BrowserViewport
   */
  getDatasetsTask = task(function*(
    this: BrowserViewport,
    op: listOp = 'set',
    start: number
  ): IterableIterator<Promise<IDatasetsGetResponse>> {
    const { prefix, platform, entity } = get(this, 'params');
    const response = yield readDatasets({ platform, prefix, start });
    const { total, count, elements } = response;
    const entities = get(this, 'entities');
    // If new segment / data-source, replace all items rather than append
    const listOp = op === 'set' ? [].setObjects : [].pushObjects;

    listOp.call(entities, elements);

    setProperties(this, {
      total,
      count,
      currentEntity: entity,
      entityRoute: `${entity}.${entity.slice(0, -1)}`,
      start: entities.length
    });

    return response;
  }).drop();

  /**
   * Async requests for the list of entities and sets the value on class
   * @type {TaskProperty<Promise<IDatasetsGetResponse>> & {perform: (a?: number) => TaskInstance<Promise<IDatasetsGetResponse>>}}
   * @memberof BrowserViewport
   */
  getEntityDataTask = task(function*(
    this: BrowserViewport,
    offset: number = 0
  ): IterableIterator<TaskInstance<Promise<IDatasetsGetResponse>>> {
    const { entity } = get(this, 'params');

    yield (<IGetEntityTaskStrategy>{
      datasets: get(this, 'getDatasetsTask').perform('set', offset)
    })[entity];
  });

  /**
   * The max possible entities that can currently be requested up to this.count
   * @type {ComputedProperty<number>}
   * @memberof BrowserViewport
   */
  nextCountSize: ComputedProperty<number> = computed('entities.length', function(this: BrowserViewport): number {
    const { entities: { length }, total, count } = getProperties(this, ['entities', 'total', 'count']);
    return Math.min(count, total - length);
  });

  /**
   * The total number of items left that can be fetched
   * @type {ComputedProperty<number>}
   * @memberof BrowserViewport
   */
  remainingEntityCount: ComputedProperty<number> = computed('entities.length', function(this: BrowserViewport): number {
    const { entities: { length }, total } = getProperties(this, ['entities', 'total']);
    return total >= length ? total - length : 0;
  });

  /**
   * Requests the next number of entities for the segment or data source
   */
  @action
  getNextEntities(this: BrowserViewport) {
    const { remainingEntityCount, start } = getProperties(this, ['remainingEntityCount', 'start']);

    // Invoke task only if there are more entities that may be available
    if (remainingEntityCount) {
      get(this, 'getDatasetsTask').perform('push', start);
    }
  }
}
