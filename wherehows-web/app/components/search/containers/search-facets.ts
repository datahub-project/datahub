import Component from '@ember/component';
import { get } from '@ember/object';
import { computed } from '@ember-decorators/object';
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
  value: string;
  label: string;
  count: number;
}

export default class SearchFacetsContainer extends Component {
  didInsertElement(this: SearchFacetsContainer) {
    get(this, 'getPlatformsTask').perform();
  }

  /**
   * The current source to narrow search results to
   * @type {DatasetPlatform}
   */
  currentSource: string;

  /**
   * Lists data platforms available to restrict search results by source
   * @type {Array<DatasetPlatform>}
   * @private
   */
  _sources: Array<DatasetPlatform> = [];

  selections: any;
  model: any;
  /**
   * Gets the available platforms and extracts a list of dataset sources
   * @type {(Task<Promise<Array<IDataPlatform>>, (a?: any) => TaskInstance<Promise<Array<IDataPlatform>>>>)}
   */
  getPlatformsTask = task(function*(this: SearchFacetsContainer): IterableIterator<Promise<Array<IDataPlatform>>> {
    const platforms: Array<IDataPlatform> = yield readPlatforms();
    const getDatasetPlatform = ({ name }: IDataPlatform): DatasetPlatform => name;
    const dataPlatforms = arrayMap(getDatasetPlatform)(platforms);

    get(this, '_sources').setObjects(dataPlatforms);
  });

  /**
   * Creates a list of options with radio props for the data platforms that can be selected as a search filter
   * @type {(ComputedProperty<Array<ISearchSourceOption>>}
   */
  @computed('_sources.[]', 'model')
  get sources(this: SearchFacetsContainer): Array<ISearchSourceOption> {
    return this._sources.map(
      (source: DatasetPlatform): ISearchSourceOption => ({
        value: source,
        label: capitalize(source),
        count: this.model.groupbysource[source] || 0
      })
    );
  }

  @computed('sources')
  get facets() {
    return [
      {
        name: 'fabric',
        displayName: 'Fabrics',
        values: [
          {
            value: 'prod',
            label: 'Prod',
            count: this.model.groupbyfabric.prod || 0
          },
          {
            value: 'corp',
            label: 'Corp',
            count: this.model.groupbyfabric.corp || 0
          },
          {
            value: 'ei',
            label: 'EI',
            count: this.model.groupbyfabric.ei || 0
          },
          {
            value: 'dev',
            label: 'Dev',
            count: this.model.groupbyfabric.dev || 0
          }
        ]
          .sortBy('count')
          .reverse()
      },
      {
        name: 'source',
        displayName: 'Data Platform',
        values: this.sources.sortBy('count').reverse()
      }
    ];
  }

  onFacetsChange(_: any) {
    //nothing
  }

  onFacetChange(facet: any, facetValue: any) {
    const currentFacetValues = this.selections[facet.name] || {};
    this.set('selections', {
      ...this.selections,
      [facet.name]: {
        ...currentFacetValues,
        [facetValue.value]: !currentFacetValues[facetValue.value]
      }
    });
    this.onFacetsChange(this.selections);
  }

  onFacetClear(facet: any) {
    this.set('selections', {
      ...this.selections,
      [facet.name]: {}
    });
    this.onFacetsChange(this.selections);
  }
}
