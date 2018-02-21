import Component from '@ember/component';
import { get, setProperties } from '@ember/object';
import { task } from 'ember-concurrency';
import { IBrowserRouteParams } from 'wherehows-web/routes/browse/entity';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { readDatasets } from 'wherehows-web/utils/api/datasets/dataset';

// Describes the index signature for strategy pattern in the getEntitiesTask
type IGetEntityTaskStrategy = { [K in IBrowserRouteParams['entity']]: Promise<Array<IDatasetView>> };

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

  didUpdateAttrs() {
    this._super(...arguments);
    get(this, 'getEntitiesTask').perform();
  }

  didInsertElement() {
    this._super(...arguments);
    get(this, 'getEntitiesTask').perform();
  }

  /**
   * Async requests for the list of entities and sets the value on class
   * @type {TaskProperty<Promise<IDatasetView[]>> & {perform: (a?: {} | undefined) => TaskInstance<Promise<IDatasetView[]>>}}
   * @memberof BrowserViewport
   */
  getEntitiesTask = task(function*(this: BrowserViewport): IterableIterator<Promise<Array<IDatasetView>>> {
    const { prefix, platform, entity } = get(this, 'params');

    const entities = (<IGetEntityTaskStrategy>{
      datasets: readDatasets({ platform, prefix }),
      metrics: Promise.resolve([]),
      flows: Promise.resolve([])
    })[entity];

    setProperties(this, {
      entities: yield entities,
      currentEntity: entity,
      entityRoute: `${entity}.${entity.slice(0, -1)}`
    });
  });
}
