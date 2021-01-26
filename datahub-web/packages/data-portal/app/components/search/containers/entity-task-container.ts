import {
  IBaseTrackingEvent,
  ICustomEventData,
  ITrackSearchResultImpressionsParams
} from '@datahub/shared/types/tracking/event-tracking.d';
import { TrackingEventCategory } from '@datahub/shared/constants/tracking/event-tracking/index';
import Component from '@ember/component';
import { set, computed } from '@ember/object';
import { facetFromParamUrl, facetToParamUrl } from '@datahub/shared/utils/search/search';
import { IFacetsCounts, IFacetsSelectionsMap } from '@datahub/data-models/types/entity/facets';
import { task } from 'ember-concurrency';
import { debounce } from '@ember/runloop';
import { IDataModelEntitySearchResult, ISearchDataWithMetadata } from '@datahub/data-models/types/entity/search';
import { DataModelEntity, DataModelName, DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { alias } from '@ember/object/computed';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { inject as service } from '@ember/service';
import RouterService from '@ember/routing/router-service';
import { fromRestli } from 'restliparams';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import UnifiedTracking from '@datahub/shared/services/unified-tracking';
import DataModelsService from '@datahub/data-models/services/data-models';
import { ITrackEntitySearchParams } from '@datahub/shared/types/tracking/search';
import { getFacetForcedValueForEntity } from '@datahub/data-models/entity/utils/facets';
import { IEntityRenderCommonPropsSearch } from '@datahub/data-models/types/search/search-entity-render-prop';
import CurrentUser from '@datahub/shared/services/current-user';
import { PageKey, CustomTrackingEventName } from '@datahub/shared/constants/tracking/event-tracking';
import { ISearchResultImpressionTrackEventParams } from '@datahub/shared/types/tracking/event-tracking';
import {
  getResultsForEntity,
  searchResultItemIndex,
  withResultMetadata
} from '@datahub/shared/utils/search/search-results';
import { assertComponentPropertyNotUndefined } from '@datahub/utils/decorators/assert';

@containerDataSource<SearchEntityTaskContainer>('searchTask', ['keyword', 'page', 'facets', 'entity'])
export default class SearchEntityTaskContainer extends Component {
  /**
   * Service to create entity instances
   */
  @service('data-models')
  dataModels!: DataModelsService;

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
   * Injects the application CurrentUser service to provide the actor information on the action event
   */
  @service('current-user')
  sessionUser!: CurrentUser;

  /**
   * User provided keyword search string
   * @type {string}
   */
  keyword?: string;

  /**
   * The current search page
   * @type {number}
   */
  page?: number;

  /**
   * Encoded facets state in a restli fashion
   * @type {string}
   */
  facets?: string;

  /**
   * The category to narrow/ filter search results
   */
  @assertComponentPropertyNotUndefined
  entity: DataModelName;

  /**
   * Page size of search results, expected to be passed in or received from a held constant
   * @type {number}
   */
  @assertComponentPropertyNotUndefined
  pageSize = 10;

  /**
   * If we wish to enable track or not
   */
  shouldCollectSearchTelemetry?: boolean;

  /**
   * Search config used to render search/facets/autocomplete
   */
  @assertComponentPropertyNotUndefined
  searchConfig: IEntityRenderCommonPropsSearch;

  /**
   * Search result data
   */
  result?: IDataModelEntitySearchResult<DataModelEntityInstance>;

  /**
   * Flag indicating that the search facets have changed and a search will be performed
   * @type {boolean}
   * @memberof SearchController
   */
  _isFacetsChanging = false;

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
  @(task(function*(this: SearchEntityTaskContainer): IterableIterator<Promise<void>> {
    return yield this.searchEntities();
  }).restartable())
  searchTask!: ETaskPromise<void>;

  /**
   * Will perform a search for any type of entity (except datasets at the moment)
   */
  getResultsForEntity(
    entity: DataModelName,
    searchConfig: SearchEntityTaskContainer['searchConfig']
  ): Promise<IDataModelEntitySearchResult<DataModelEntityInstance> | undefined> {
    const forcedFacets = getFacetForcedValueForEntity(searchConfig.attributes);

    return getResultsForEntity(
      {
        facetsApiParams: { ...this.facetsApiParams, ...forcedFacets },
        keyword: this.keyword || '',
        page: this.page || 1,
        pageSize: this.pageSize,
        aspects: searchConfig.defaultAspects
      },
      entity,
      this.dataModels
    );
  }

  /**
   * Used as an entry to the getResultsForEntity method from our data task in this component
   */
  async searchEntities(): Promise<void> {
    const { entity, searchConfig } = this;
    const result = await this.getResultsForEntity(entity, searchConfig);
    if (result) {
      const trackingParams = this.getEntitySearchTrackingParams(result);
      trackingParams && this.trackEntitySearch(trackingParams);
      set(this, 'result', result);
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
    const { entity, dataModels } = this;
    const entityClass = dataModels.getModel(entity || DatasetEntity.displayName);
    return entityClass;
  }

  /**
   * From an entity search result, derives the parameters needed for tracking results
   * @param searchResults - the expected results from performing an entity search
   */
  getEntitySearchTrackingParams(searchResults: SearchEntityTaskContainer['result']): ITrackEntitySearchParams | void {
    if (searchResults) {
      const { keyword = '', entity } = this;
      const { itemsPerPage, page } = searchResults;
      const searchResultImpressionsTrackingParams: ITrackSearchResultImpressionsParams = {
        result: searchResults,
        itemsPerPage,
        page
      };

      return { keyword, entity, searchCount: searchResults.count, searchResultImpressionsTrackingParams };
    }
  }

  /**
   * Uses the injected analytics service to track the current search performed
   * @param {ITrackEntitySearchParams} entitySearchTrackingParams parameters supplied to the tracking service's trackEntitySearch method
   */
  trackEntitySearch(entitySearchTrackingParams: ITrackEntitySearchParams): void {
    if (this.shouldCollectSearchTelemetry) {
      const { keyword, entity, searchCount, searchResultImpressionsTrackingParams } = entitySearchTrackingParams;
      // Allow analytics service to track content impressions , this is specific to piwik tracking
      this.trackingService.trackContentImpressions();
      // Capture search activity once search query is complete and results / no results are returned
      this.trackingService.trackSiteSearch({ keyword, entity, searchCount });
      /**
       * Tracks search result impressions for every individual search-result.
       * Refer to`trackSearchResultImpressions` method for details on what exactly is tracked under each searchResult
       */
      this.trackSearchResultImpressions(searchResultImpressionsTrackingParams);
    }
  }

  /**
   * data massage needed to transform search result api structure into something that the
   * UI can consume better. For example, it will contain the data for the entity links like route
   * and query params.
   */
  @computed('result')
  get decoratedResults(): undefined | IDataModelEntitySearchResult<ISearchDataWithMetadata<DataModelEntityInstance>> {
    const { result } = this;
    if (result) {
      const { page, itemsPerPage } = result;

      // Decorates each result item with ISearchResultMetadata metadata
      const resultsDataWithMetadata: Array<ISearchDataWithMetadata<DataModelEntityInstance>> = result.data.map(
        withResultMetadata({ page, itemsPerPage })
      );
      // now data instead of plain api is decorated data
      return { ...result, data: resultsDataWithMetadata };
    }

    return;
  }

  /**
   * Method used to iterate over the search results of an entity and handoff `userName`, `targetUrn` and `absolutePosition` for each result to the
   * `trackSearchResultImpressionEvent` method.
   *
   * @param {ITrackSearchResultImpressionsParams} trackSearchResultImpressionParams A map containing the search result items , itemsPerPage and the page number
   */
  trackSearchResultImpressions(trackSearchResultImpressionParams: ITrackSearchResultImpressionsParams): void {
    const userName = this.sessionUser.entity?.username || '';
    const { itemsPerPage, result, page } = trackSearchResultImpressionParams;
    // iterates over the search results of the current page and tracks the impression of each result through their absolutePosition and urn
    result.data.forEach((searchResultItem: { urn: string }, pageIndex: number): void => {
      const absolutePosition = searchResultItemIndex({ pageIndex, itemsPerPage, page });
      const targetUrn = searchResultItem.urn;
      this.trackSearchResultImpressionEvent({ userName, targetUrn, absolutePosition });
    });
  }

  /**
   * Creates attributes for tracking the search action event and invokes the tracking service.
   *
   * @param {ICategoryContainerTrackEventParams} { userName, query, absolutePosition, facet } the current user's username, search query keyed in by the user , the positioning of the search result card in the list, the facet filters clicked by the user
   */
  trackSearchResultImpressionEvent({
    userName,
    targetUrn,
    absolutePosition
  }: ISearchResultImpressionTrackEventParams): void {
    const baseEventAttrs: IBaseTrackingEvent = {
      category: TrackingEventCategory.Search,
      action: CustomTrackingEventName.SearchImpression
    };
    const customEventAttrs: ICustomEventData = {
      pageKey: PageKey.searchCategory,
      eventName: CustomTrackingEventName.SearchImpression,
      userName: userName,
      target: targetUrn,
      body: {
        requesterUrn: userName,
        targetUrn: targetUrn,
        query: this.keyword,
        absolutePosition,
        facet: null
      }
    };
    this.trackingService.trackEvent(baseEventAttrs, customEventAttrs);
  }
}
