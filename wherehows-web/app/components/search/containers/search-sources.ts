import Component from '@ember/component';
import { get, computed } from '@ember/object';
import { capitalize } from '@ember/string';
import { task } from 'ember-concurrency';
import { DatasetPlatform } from 'wherehows-web/constants';
import { IDataPlatform } from 'wherehows-web/typings/api/list/platforms';
import { readPlatforms } from 'wherehows-web/utils/api/list/platforms';
import { arrayMap } from 'wherehows-web/utils/array';

/**
 * Describes the interface of the object passed as radio option for source
 * @interface ISearchSourceOption
 */
interface ISearchSourceOption {
  name: 'source';
  value: DatasetPlatform;
  label: string;
  group: string;
}

export default class SearchSourcesContainer extends Component {
  didInsertElement(this: SearchSourcesContainer) {
    get(this, 'getPlatformsTask').perform();
  }

  /**
   * The current source to narrow search results to
   * @type {DatasetPlatform}
   */
  currentSource: DatasetPlatform;

  /**
   * Lists data platforms available to restrict search results by source
   * @type {Array<DatasetPlatform>}
   * @private
   */
  _sources: Array<DatasetPlatform> = [];

  /**
   * Gets the available platforms and extracts a list of dataset sources
   * @type {(Task<Promise<Array<IDataPlatform>>, (a?: any) => TaskInstance<Promise<Array<IDataPlatform>>>>)}
   */
  getPlatformsTask = task(function*(this: SearchSourcesContainer): IterableIterator<Promise<Array<IDataPlatform>>> {
    const platforms: Array<IDataPlatform> = yield readPlatforms();
    const getDatasetPlatform = ({ name }: IDataPlatform): DatasetPlatform => name;
    const dataPlatforms = arrayMap(getDatasetPlatform)(platforms);

    get(this, '_sources').setObjects(dataPlatforms);
  });

  /**
   * Creates a list of options with radio props for the data platforms that can be selected as a search filter
   * @type {(ComputedProperty<Array<ISearchSourceOption>>}
   */
  sources = computed('currentSource', '_sources.[]', function(
    this: SearchSourcesContainer
  ): Array<ISearchSourceOption> {
    const allSource = <DatasetPlatform>'all';
    const sourceAsOption = (source: DatasetPlatform): ISearchSourceOption => ({
      name: 'source',
      value: source,
      label: capitalize(source),
      group: String(get(this, 'currentSource')).toLowerCase()
    });

    return arrayMap(sourceAsOption)([allSource, ...get(this, '_sources')]);
  });
}
