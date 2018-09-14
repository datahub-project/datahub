import Component from '@ember/component';
import { get } from '@ember/object';
import { computed } from '@ember-decorators/object';
import { capitalize } from '@ember/string';
import { task } from 'ember-concurrency';
import { DatasetPlatform } from 'wherehows-web/constants';
import { IDataPlatform } from 'wherehows-web/typings/api/list/platforms';
import { readPlatforms } from 'wherehows-web/utils/api/list/platforms';
import { arrayMap } from 'wherehows-web/utils/array';
import { IFacetsSelectionsMap, IFacetsCounts } from 'wherehows-web/utils/api/search';

/**
 * Options inside a facet
 * @interface ISearchFacetOption
 */
interface ISearchFacetOption {
  value: string;
  label: string;
}

/**
 * Interface of a facet
 */
interface ISearchFacet {
  name: string;
  displayName: string;
  values: Array<ISearchFacetOption>;
}

/**
 * Container component for search facets
 * It will store state related to search facets.
 */
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

  /**
   * Current state of selections of facets
   * EI:
   * {
   *  source: {
   *    hdfs: true
   *  },
   *  fabric: {
   *    corp: true,
   *    prod: true
   *  }
   * }
   */
  selections: IFacetsSelectionsMap;

  /**
   * Counts for the facets in a similar fashion of selections
   */
  counts: IFacetsCounts;

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
   * I will convert a string into a facet option with counts
   * @param facetValue
   * @param facetName
   */
  stringToFacetOption(facetValue: string): ISearchFacetOption {
    return {
      value: facetValue,
      label: capitalize(facetValue)
    };
  }

  /**
   * Creates a list of options with radio props for the data platforms that can be selected as a search filter
   * @type {(ComputedProperty<Array<ISearchSourceOption>>}
   */
  @computed('_sources.[]', 'counts')
  get sources(this: SearchFacetsContainer): Array<ISearchFacetOption> {
    return this._sources.map(source => this.stringToFacetOption(source));
  }

  /**
   * Facets that are available right now.
   * In the future, it should be fetched from the backend
   */
  @computed('sources', 'counts')
  get facets(): Array<ISearchFacet> {
    return [
      {
        name: 'fabric',
        displayName: 'Fabrics',
        values: ['prod', 'corp', 'ei', 'dev'].map(fabric => this.stringToFacetOption(fabric))
      },
      {
        name: 'source',
        displayName: 'Data Platform',
        values: this.sources
      }
    ];
  }

  /**
   * External closure action that triggers when facet changes
   * @param _ Facet Selections
   */
  onFacetsChange(_: IFacetsSelectionsMap) {
    //nothing
  }

  /**
   * Internal action triggered when facet changes. It will update
   * the state of selections in a redux fashion.
   * @param facet The facet that changed
   * @param facetValue the option of the facet that changed
   */
  onFacetChange(facet: ISearchFacet, facetValue: ISearchFacetOption) {
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

  /**
   * When the user clear the facet
   * @param facet the facet that the user selects
   */
  onFacetClear(facet: ISearchFacet) {
    this.set('selections', {
      ...this.selections,
      [facet.name]: {}
    });
    this.onFacetsChange(this.selections);
  }
}
