import Component from '@ember/component';
import { get, set } from '@ember/object';
import { computed } from '@ember/object';
import { capitalize } from '@ember/string';
import { arrayMap } from 'wherehows-web/utils/array';
import {
  ISearchFacet,
  ISearchFacetOption,
  IFacetsSelectionsMap,
  IFacetsCounts
} from '@datahub/data-models/types/entity/facets';
import { task } from 'ember-concurrency';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/entity/rendering/search-entity-render-prop';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { IDataPlatform } from '@datahub/metadata-types/types/entity/dataset/platform';
import { readDataPlatforms } from '@datahub/data-models/api/dataset/platforms';
import { ETaskPromise } from '@datahub/utils/types/concurrency';

/**
 * Container component for search facets
 * It will store state related to search facets.
 */
export default class SearchFacetsContainer extends Component {
  didInsertElement(): void {
    this.getPlatformsTask.perform();
  }

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
  selections!: IFacetsSelectionsMap;

  /**
   * Counts for the facets in a similar fashion of selections
   */
  counts!: IFacetsCounts;

  /**
   * Fields available. We will read this to see what facets do we have available
   */
  fields: Array<ISearchEntityRenderProps> = [];

  /**
   * Gets the available platforms and extracts a list of dataset sources
   * @returns {IterableIterator<Promise<Array<IDataPlatform>>>}
   * @memberof SearchFacetsContainer
   */
  @task(function*(this: SearchFacetsContainer): IterableIterator<Promise<Array<IDataPlatform>>> {
    const platforms: Array<IDataPlatform> = yield readDataPlatforms();
    const getDatasetPlatform = ({ name }: IDataPlatform): DatasetPlatform => name;
    const dataPlatforms = arrayMap(getDatasetPlatform)(platforms);

    get(this, '_sources').setObjects(dataPlatforms);
  })
  getPlatformsTask!: ETaskPromise<Array<IDataPlatform>>;

  /**
   * I will convert a string into a facet option with counts
   * @param facetValue
   * @param facetName
   */
  stringToFacetOption(facetValue: string): ISearchFacetOption {
    return {
      value: facetValue,
      label: capitalize(facetValue),
      count: 0
    };
  }

  /**
   * Creates a list of options with radio props for the data platforms that can be selected as a search filter
   * @type {(ComputedProperty<Array<ISearchSourceOption>>}
   */
  @computed('_sources.[]', 'counts')
  get sources(): Array<ISearchFacetOption> {
    return this._sources.map((source): ISearchFacetOption => this.stringToFacetOption(source));
  }

  /**
   * Facets that are available right now.
   * In the future, it should be fetched from the backend
   */
  @computed('counts', 'fields')
  get facets(): Array<ISearchFacet> {
    const counts: IFacetsCounts = this.counts || {};
    const facets: Array<ISearchFacet> = this.fields
      .filterBy('showInFacets')
      .map(
        (field): ISearchFacet => {
          const fieldCounts = counts[field.fieldName] || {};
          return {
            name: field.fieldName,
            displayName: field.displayName,
            values: Object.keys(fieldCounts)
              .map(
                (value): ISearchFacetOption => ({
                  ...this.stringToFacetOption(value),
                  count: fieldCounts[value]
                })
              )
              .sortBy('count')
              .reverse()
          };
        }
      )
      .filter((field): boolean => field.values.length > 0);

    return facets;
  }

  /**
   * External closure action that triggers when facet changes
   * @param _ Facet Selections
   */
  onFacetsChange(_: IFacetsSelectionsMap): void {
    //nothing
  }

  /**
   * Internal action triggered when facet changes. It will update
   * the state of selections in a redux fashion.
   * @param facet The facet that changed
   * @param facetValue the option of the facet that changed
   */
  onFacetChange(facet: ISearchFacet, facetValue: ISearchFacetOption): void {
    const currentFacetValues = this.selections[facet.name] || {};
    set(this, 'selections', {
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
  onFacetClear(facet: ISearchFacet): void {
    set(this, 'selections', {
      ...this.selections,
      [facet.name]: {}
    });
    this.onFacetsChange(this.selections);
  }
}
