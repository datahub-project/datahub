import {
  PageKey,
  CustomTrackingEventName,
  SearchActionEventCategory
} from '@datahub/shared/constants/tracking/event-tracking/index';
import Component from '@ember/component';
import { get, set } from '@ember/object';
import { computed } from '@ember/object';
import { capitalize } from '@ember/string';
import { arrayMap } from '@datahub/utils/array/index';
import {
  ISearchFacet,
  ISearchFacetOption,
  IFacetsSelectionsMap,
  IFacetsCounts
} from '@datahub/data-models/types/entity/facets';
import { task } from 'ember-concurrency';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { IDataPlatform } from '@datahub/metadata-types/types/entity/dataset/platform';
import { readDataPlatforms } from '@datahub/data-models/api/dataset/platforms';
import { ETaskPromise } from '@datahub/utils/types/concurrency';

import UnifiedTracking from '@datahub/shared/services/unified-tracking';
import { inject as service } from '@ember/service';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/search/search-entity-render-prop';
import {
  IBaseTrackingEvent,
  ICustomEventData,
  ISearchActionFacetClickParams
} from '@datahub/shared/types/tracking/event-tracking';
import CurrentUser from '@datahub/shared/services/current-user';
import { searchTrackingEvent } from '@datahub/shared/constants/tracking/event-tracking/search';

/**
 * Container component for search facets
 * It will store state related to search facets.
 */
export default class SearchFacetsContainer extends Component {
  /**
   * Injects the application CurrentUser service to provide the actor information on the action event
   */
  @service('current-user')
  sessionUser!: CurrentUser;

  didInsertElement(): void {
    this.getPlatformsTask.perform();
  }

  @service('unified-tracking')
  tracking!: UnifiedTracking;

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
   * Keyword or query for the current search
   */
  keyword = '';

  /**
   * Counts for the facets in a similar fashion of selections
   */
  counts!: IFacetsCounts;

  /**
   * Fields available. We will read this to see what facets do we have available
   */
  fields: Array<ISearchEntityRenderProps>;

  /**
   * Gets the available platforms and extracts a list of dataset sources
   * @returns {IterableIterator<Promise<Array<IDataPlatform>>>}
   * @memberof SearchFacetsContainer
   */
  @task(function*(this: SearchFacetsContainer): IterableIterator<Promise<Array<IDataPlatform>>> {
    const platforms = ((yield readDataPlatforms()) as unknown) as Array<IDataPlatform>;
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
    const searchFacets: Array<ISearchFacet> = this.fields
      .filterBy('showInFacets')
      .map(
        (field): ISearchFacet => {
          const fieldCounts = counts[field.fieldName] || {};
          let searchFacet: ISearchFacet = {
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

          // Each searchFacet can optionally contain this customizable field ( through render props ),
          // in that case we append it to the searchFacet object while mapping the rest of the values such as name and values
          if (field.headerComponent) {
            searchFacet = { ...searchFacet, headerComponent: field.headerComponent };
          }
          return searchFacet;
        }
      )
      .filter((field): boolean => field.values.length > 0);
    return searchFacets;
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
   * the state of selections in a redux fashion. It also handles firing of track events for facetClicks
   *
   * @param facet The facet that changed
   * @param facetValue the option of the facet that changed
   */
  onFacetChange(facet: ISearchFacet, facetValue: ISearchFacetOption): void {
    // Triggers the tracking of the facet click
    const searchActionFacetClickParams: ISearchActionFacetClickParams = {
      facetName: facet.name,
      facetValue: facetValue.value
    };
    this.trackSearchActionFacetClickEvent(searchActionFacetClickParams);

    // Update the state of selections
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

  /**
   * Creates attributes for tracking the search action event and  invokes the tracking service
   * @param {ISearchActionFacetClickParams} searchActionFacetClickParams A map containing the current user's username, the facet and the facetValue that the user clicked on
   */
  trackSearchActionFacetClickEvent(searchActionFacetClickParams: ISearchActionFacetClickParams): void {
    const userName = this.sessionUser.entity?.username || '';
    const { facetName, facetValue } = searchActionFacetClickParams;

    const baseEventAttrs: IBaseTrackingEvent = {
      ...searchTrackingEvent.SearchResultFacetClick,
      name: facetName,
      value: facetValue
    };

    const customEventAttrs: ICustomEventData = {
      pageKey: PageKey.searchCategory,
      eventName: CustomTrackingEventName.SearchAction,
      userName,
      target: facetValue,
      body: {
        requesterUrn: userName,
        targetUrn: facetValue,
        actionCategory: SearchActionEventCategory.FacetClick,
        query: this.keyword,
        absolutePosition: null,
        facet: facetName
      }
    };
    this.tracking.trackEvent(baseEventAttrs, customEventAttrs);
  }
}
