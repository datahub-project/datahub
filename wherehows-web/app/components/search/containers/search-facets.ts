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
  group: string;
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
  currentSource: DatasetPlatform;

  /**
   * Lists data platforms available to restrict search results by source
   * @type {Array<DatasetPlatform>}
   * @private
   */
  _sources: Array<DatasetPlatform> = [];

  selections: any = {
    fabric: {
      EI: true
    }
  };

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
  @computed('currentSource', '_sources.[]')
  get sources(this: SearchFacetsContainer): Array<ISearchSourceOption> {
    const sourceAsOption = (source: DatasetPlatform): ISearchSourceOption => ({
      value: source,
      label: capitalize(source),
      group: String(get(this, 'currentSource')).toLowerCase(),
      count: 0
    });

    return arrayMap(sourceAsOption)(get(this, '_sources'));
  }

  @computed('sources')
  get facets() {
    return [
      {
        name: 'fabric',
        displayName: 'Fabrics',
        values: [
          {
            value: 'PROD',
            label: 'Prod',
            count: 0
          },
          {
            value: 'CORP',
            label: 'Corp',
            count: 0
          },
          {
            value: 'EI',
            label: 'EI',
            count: 0
          },
          {
            value: 'DEV',
            label: 'Dev',
            count: 0
          }
        ]
      },
      {
        name: 'source',
        displayName: 'Data Platform',
        values: this.sources
      }
    ];
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
  }

  onFacetClear(facet: any) {
    this.set('selections', {
      ...this.selections,
      [facet.name]: {}
    });
  }
}
