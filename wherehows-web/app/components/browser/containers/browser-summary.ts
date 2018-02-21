import Component from '@ember/component';
import { get, setProperties } from '@ember/object';
import { task, TaskInstance } from 'ember-concurrency';
import { IBrowserRouteParams } from 'wherehows-web/routes/browse/entity';
import { readDatasetsCount } from 'wherehows-web/utils/api/datasets/dataset';

// Describes the index signature for the metadata object used in the browser summary component
type IBrowserMetadata = {
  [K in IBrowserRouteParams['entity']]: {
    count: number;
    currentPlatform: string;
  }
};

// Describes the index signature for the strategy pattern in the getCountsTask
type ICountTaskStrategy = { [K in IBrowserRouteParams['entity']]: TaskInstance<Promise<number>> };

export default class BrowserSummary extends Component {
  /**
   * Passed in parameters containing route or query parameters values to be used in request
   * @type {IBrowserRouteParams}
   * @memberof BrowserSummary
   */
  params: IBrowserRouteParams;

  /**
   * Lists the types of entities supported
   * @type {ReadonlyArray<string>}
   * @memberof BrowserSummary
   */
  entities: ReadonlyArray<IBrowserRouteParams['entity']> = ['datasets', 'metrics', 'flows'];

  /**
   * Contains the properties for each entity to displayed UI metadata
   * @type {IBrowserMetadata}
   * @memberof BrowserSummary
   */
  metadata: IBrowserMetadata = this.entities.reduce(
    (metadata, entity) => ({
      ...metadata,
      [entity]: { count: 0, currentPlatform: '' }
    }),
    <IBrowserMetadata>{}
  );

  didUpdateAttrs() {
    this._super(...arguments);
    get(this, 'getCountsTask').perform();
  }

  didInsertElement() {
    this._super(...arguments);
    get(this, 'getCountsTask').perform();
  }

  /**
   * Parent task to retrieve counts for each IBrowserParams entity type as needed
   * @type {TaskProperty<TaskInstance<Promise<number>>> & {perform: (a?: {} | undefined) => TaskInstance<TaskInstance<Promise<number>>>}}
   * @memberof BrowserSummary
   */
  getCountsTask = task(function*(this: BrowserSummary): IterableIterator<TaskInstance<Promise<number>>> {
    const { prefix, platform, entity } = get(this, 'params');

    return (<ICountTaskStrategy>{
      datasets: yield get(this, 'getDatasetsCountTask').perform(platform, prefix)
    })[entity];
  });

  /**
   * Gets and sets the dataset count
   * @type {TaskProperty<Promise<number>> & {perform: (a1: string, a2: string) => TaskInstance<Promise<number>>}}
   * @memberof BrowserSummary
   */
  getDatasetsCountTask = task(function*(
    this: BrowserSummary,
    platform: string,
    prefix: string
  ): IterableIterator<Promise<number>> {
    const entityMetadata = get(get(this, 'metadata'), 'datasets');

    setProperties(entityMetadata, {
      count: yield readDatasetsCount({ prefix, platform }),
      currentPlatform: platform
    });
  });
}
