import Component from '@ember/component';
import { set } from '@ember/object';
import { computed } from '@ember/object';
import { facetFromParamUrl, readSearchV2, facetToParamUrl } from 'wherehows-web/utils/api/search/search';
import { IFacetsCounts, IFacetsSelectionsMap } from '@datahub/data-models/types/entity/facets';
import { task } from 'ember-concurrency';
import { debounce } from '@ember/runloop';
import { IDataModelEntitySearchResult, ISearchDataWithMetadata } from '@datahub/data-models/types/entity/search';
import { DataModelEntity, DataModelName } from '@datahub/data-models/constants/entity';
import { withResultMetadata, searchResultMetasToFacetCounts } from 'wherehows-web/utils/search/search-results';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { ISearchEntityApiParams, IEntitySearchResult } from 'wherehows-web/typings/api/search/entity';
import { alias } from '@ember/object/computed';
import { IBaseEntity } from '@datahub/metadata-types/types/entity';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { inject as service } from '@ember/service';
import RouterService from '@ember/routing/router-service';
import { fromRestli } from 'restliparams';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/entity/rendering/search-entity-render-prop';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import UnifiedTracking from '@datahub/tracking/services/unified-tracking';

@containerDataSource('searchTask', ['keyword', 'page', 'facets', 'entity'])
export default class SearchEntityTaskContainer<T extends IBaseEntity> extends Component {
  /**
   * Router service to perform some changes in query params
   */
  @service
  router: RouterService;

  /**
   * References the application tracking service which is used for analytics activation, setup, and management
   */
  @service('unified-tracking')
  trackingService: UnifiedTracking;

  /**
   * User provided keyword search string
   * @type {string}
   */
  keyword: string;

  /**
   * The current search page
   * @type {number}
   */
  page: number;

  /**
   * Encoded facets state in a restli fashion
   * @type {string}
   */
  facets: string;

  /**
   * The category to narrow/ filter search results
   * @type {string}
   */
  entity: DataModelName;

  /**
   * Page size of search results, expected to be passed in or received from a held constant
   * @type {number}
   */
  pageSize: number = 10;

  /**
   * If we wish to enable track or not
   */
  shouldCollectSearchTelemetry: boolean = false;

  /**
   * Fields of the entity to render in search/facets/autocomplete
   */
  fields: Array<ISearchEntityRenderProps>;

  /**
   * Search result data
   */
  result?: IDataModelEntitySearchResult<T>;

  /**
   * Flag indicating that the search facets have changed and a search will be performed
   * @type {boolean}
   * @memberof SearchController
   */
  _isFacetsChanging: boolean = false;

  /**
   * Counts for all the facets showing in the ui from search result
   */
  @alias('result.facets')
  facetCounts: IFacetsCounts;

  /**
   * Determines if this controller is busy querying the search endpoint of performing a related task async.
   * Currently this is true if any of the following is truthy:
   * 1. searchTask.isRunning === true
   * 2. Facets are changed and the operation is being debounced, _isFacetsChanging === true
   * @readonly
   * @memberof SearchController
   */
  @computed('searchTask.isRunning', '_isFacetsChanging')
  get isSearchRunning(): boolean {
    return this.searchTask.isRunning || this._isFacetsChanging;
  }

  /**
   * Will read selections from URL and translate it into our selections object
   * @type {IFacetsSelectionsMap}
   */
  @computed('facets')
  get facetsSelections(): IFacetsSelectionsMap {
    return facetFromParamUrl(this.facets || '');
  }

  /**
   * This fn will generate a record of string (field name) of string array (selections)
   * for the api. Also, it will make sure some 'hidden' facets are sent too.
   */
  @computed('facets', 'fields')
  get facetsApiParams(): Record<string, Array<string>> {
    const { facets } = this;
    const facetsApiParams: Record<string, Array<string>> = facets ? fromRestli(facets) : {};
    return facetsApiParams;
  }

  /**
   * Will return false if there is data to display
   */
  @computed('result.data.length')
  get showNoResult(): boolean {
    const { result } = this;

    return result && result.data ? !result.data.length : false;
  }

  /**
   * This function will change the current route with the params needed when
   * facets changes
   */
  onFacetsParamChange(selections: IFacetsSelectionsMap): void {
    this.router.transitionTo(this.router.currentRouteName, {
      queryParams: {
        facets: facetToParamUrl(selections),
        page: 1
      }
    });
  }

  /**
   * When facets change we set the flag loading and call the debounced fn
   * @param selections facet selections
   */
  onFacetsChange(selections: IFacetsSelectionsMap): void {
    set(this, '_isFacetsChanging', true);
    this.onFacetsChangeDebounced(selections);
  }

  /**
   * Will set the facets in the URL to start a model refresh (see route)
   * @param selections Facet selections
   */
  onFacetsChangeDebounced(selections: IFacetsSelectionsMap): void {
    debounce(this, this.onFacetsDidChange, selections, 1000);
  }

  /**
   * Debounced auxiliary fn from onFacetsChangeDebounced
   * @param selections
   */
  onFacetsDidChange(selections: IFacetsSelectionsMap): void {
    set(this, '_isFacetsChanging', false);
    this.onFacetsParamChange(selections);
  }

  /**
   * Queries the search endpoint with the requested query params and applies the search result to the result
   * attribute
   */
  @(task(function*(this: SearchEntityTaskContainer<T>): IterableIterator<Promise<void>> {
    return yield this.searchEntities();
  }).restartable())
  searchTask!: ETaskPromise<void>;
  /**
   * Will perform a search for all types of entities (except datasets at the moment)
   */
  async searchEntities(): Promise<void> {
    const { keyword, page, facetsApiParams, entity, pageSize, shouldCollectSearchTelemetry } = this;

    const searchApiParams: ISearchEntityApiParams = {
      facets: facetsApiParams,
      input: keyword,
      type: this.dataModelEntity.renderProps.search.apiName,
      start: (page - 1) * pageSize,
      count: pageSize
    };

    const searchResultProxy: IEntitySearchResult<T> = await readSearchV2<T>(searchApiParams);
    const { elements } = searchResultProxy;

    if (elements) {
      const { start, count, total } = searchResultProxy;
      const itemsPerPage = count;
      const totalPages = Math.ceil(total / itemsPerPage);
      const page = Math.ceil((start + 1) / itemsPerPage);

      set(this, 'result', {
        data: elements,
        count: total,
        start,
        itemsPerPage,
        page,
        totalPages,
        facets: searchResultMetasToFacetCounts(searchResultProxy.searchResultMetadatas, facetsApiParams)
      });

      if (shouldCollectSearchTelemetry) {
        // Allow analytics service to track content impressions
        this.trackingService.trackContentImpressions();
        // Capture search activity once search query is complete and results / no results are returned
        this.trackingService.trackSiteSearch({ keyword, entity, searchCount: total });
      }

      return;
    }

    // result is nullable, but null when there is an exception at the search endpoint
    throw new Error('Could not parse search results');
  }

  /**
   * Relevant data model entity based on the kind of entity we are using. This helps us understand what
   * to render in the search results and how the autocomplete should work
   */
  @computed('entity')
  get dataModelEntity(): DataModelEntity {
    const { entity } = this;
    const entityClass = DataModelEntity[entity || DatasetEntity.displayName];
    return entityClass;
  }

  /**
   * data massage needed to transform search result api structure into something that the
   * UI can consume better. For example, it will contain the data for the entity links like route
   * and query params.
   */
  @computed('result')
  get decoratedResults(): undefined | IDataModelEntitySearchResult<ISearchDataWithMetadata<T>> {
    const { result, entity } = this;

    if (result) {
      const { page, itemsPerPage } = result;

      // Decorates each result item with ISearchResultMetadata metadata
      const resultsDataWithMetadata: Array<ISearchDataWithMetadata<T>> = result.data.map(
        withResultMetadata({ page, itemsPerPage, entity })
      );

      // now data instead of plain api is decorated data
      return { ...result, data: resultsDataWithMetadata };
    }

    return;
  }
}
