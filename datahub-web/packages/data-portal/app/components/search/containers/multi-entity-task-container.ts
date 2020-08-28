import SearchEntityTaskContainer from 'datahub-web/components/search/containers/entity-task-container';
import { DataModelName, DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { task } from 'ember-concurrency';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { IDataModelEntitySearchResult } from '@datahub/data-models/types/entity/search';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { action, setProperties, computed } from '@ember/object';
import { inject as service } from '@ember/service';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { noop } from 'lodash';
import { isSearchable } from '@datahub/shared/utils/search/entities';
import { zipObject, mapValues } from 'lodash';
import FoxieService from '@datahub/shared/services/foxie';
import { UserFunctionType } from '@datahub/shared/constants/foxie/user-function-type';

// TODO: [META-12059] MES is missing proper component and acceptance testing
// Specifically, that hidden tabs do not generate a search track event
// - you can stub the service and assert that the method is only called for the active MES tab
// - called when a tab is navigated to
// - only called when the search is successful

// Shortcut for a very long typing
type GenericSearchResult = IDataModelEntitySearchResult<DataModelEntityInstance>;

/**
 * The SearchMultiEntityTaskContainer is used in place of the entity-task-container when we want to
 * perform a search across all entities. It yields the same parameters and provides extra
 * information about the overall context of our search through the tab navigation it provides.
 *
 * Container flow is as follows:
 * containerDataSource activates getContainerDataTask upon change of parameters
 * => getContainerDataTask fetches information specifically for the entity & parameters provided
 * => if we haven't gotten counts for the overall entities yet, then run
 *    getMultiEntitySearchResultsTask and set the counts for each entity
 *      -> The counts are used to "disable" tabs that have a count of 0
 * => getContainerDataTask sets the currentResults
 * => a listener exists on the base container that will decorate our results and pass it through
 * => Remaining data and action concerns are handled by the base entity task container
 */
@containerDataSource<SearchMultiEntityTaskContainer>('getContainerDataTask', ['entity', 'keyword', 'page', 'facets'])
export default class SearchMultiEntityTaskContainer extends SearchEntityTaskContainer {
  @service
  foxie!: FoxieService;

  /**
   * External action provided to allow us to change the current entity to a new one
   */
  onChangeEntity: (newEntity: DataModelName) => void = noop;

  /**
   * From the data models service, get the list of available (unguarded entities)
   */
  get availableEntities(): Array<DataModelName> {
    return this.dataModels.guards.unGuardedEntities
      .filter(isSearchable)
      .map((entity): DataModelName => entity.displayName);
  }

  /**
   * Count returned for all the records per entity that match the search term provided
   */
  @computed('multiEntityResults')
  get entitySearchCounts(): Partial<Record<DataModelName, number>> | undefined {
    const { multiEntityResults } = this;

    if (multiEntityResults) {
      return mapValues(multiEntityResults, (result): number => (result && result.count) || 0);
    }

    return;
  }

  /**
   * If no entity is provided, we default to our default entity to direct focus to for the search
   */
  defaultEntity = DatasetEntity.displayName;

  /**
   * Object aggregating any errors that exist for a specified entity. Used to allow users to enter
   * a tab even if it has no "count" (due to request erroring before retrieving one). This way,
   * they can go to the tab themselves still. Switching to the tab will retry the call since
   * nothing was cached, and if it still errors out then they will receive a message from the
   * main task failing
   */
  aggregatedErrors: Partial<Record<DataModelName, Error>> = {};

  /**
   * Results from performing the multi-entity search task, used to grab the counts for each entity
   */
  multiEntityResults?: Partial<Record<DataModelName, GenericSearchResult>>;

  /**
   * Tracks the previously used keyword to determine whether or not we should change our current
   * multi-entity search task to rerun
   */
  previousKeyword?: string;

  /**
   * Fetches the results from search for a particular entity (using the provided params for page
   * keyword and facets). Reads from cache instead if we've performed this search before
   * @param {DataModelName} entity - the identifier for the entity for which to fetch search
   *  results
   */
  readResultsForEntity(entity: DataModelName): Promise<GenericSearchResult | void> {
    const { dataModels } = this;
    const entityClass = dataModels.getModel(entity);
    return this.getResultsForEntity(entity, entityClass.renderProps.search);
  }

  /**
   * Reads the initial search result for the specified entity (and then reads the results and
   * counts for all related entities if it hasn't been done since component init yet)
   */
  @(task(function*(this: SearchMultiEntityTaskContainer): IterableIterator<Promise<GenericSearchResult | void>> {
    const { entity, defaultEntity, keyword, previousKeyword } = this;
    const priorityEntity = entity || defaultEntity;
    const resultForPriorityEntity = ((yield this.readResultsForEntity(
      priorityEntity
    )) as unknown) as IDataModelEntitySearchResult<DataModelEntityInstance>;

    if (resultForPriorityEntity && resultForPriorityEntity.data.length < 10) {
      this.foxie.launchUFO({
        functionType: UserFunctionType.ApiResponse,
        functionTarget: 'search',
        functionContext: `empty ${entity} result`
      });
    }

    const trackingParams = this.getEntitySearchTrackingParams(resultForPriorityEntity);
    trackingParams && this.trackEntitySearch(trackingParams);

    const keywordHasChanged = keyword !== previousKeyword;

    setProperties(this, {
      result: resultForPriorityEntity,
      previousKeyword: keyword
    });

    if (!this.entitySearchCounts || keywordHasChanged) {
      this.getMultiEntitySearchResultsTask.perform();
    }
  }).restartable())
  getContainerDataTask!: ETaskPromise<void>;

  /**
   * Reads all the search results for every entity available (and saves to the cache), and pulls
   * the count metadata from each to understand how to render our tabs' meta info for users
   */
  @(task(function*(this: SearchMultiEntityTaskContainer): IterableIterator<Promise<Array<GenericSearchResult | void>>> {
    const { availableEntities } = this;
    const aggregatedErrors: SearchMultiEntityTaskContainer['aggregatedErrors'] = {};

    // This mapper function catches errors so that we don't fail the whole task if one search call
    // has issues
    const readResultsForAvailableEntity = async (entity: DataModelName): Promise<GenericSearchResult | void> => {
      try {
        const result = await this.readResultsForEntity(entity);
        return result;
      } catch (e) {
        aggregatedErrors[entity] = e;
      }
    };

    const availableEntitiesResults = ((yield Promise.all(
      availableEntities.map(readResultsForAvailableEntity)
    )) as unknown) as Array<GenericSearchResult | void>;
    const resultsMappedToEntity = zipObject(availableEntities, availableEntitiesResults);

    setProperties(this, {
      aggregatedErrors,
      multiEntityResults: resultsMappedToEntity
    });
  }).restartable())
  getMultiEntitySearchResultsTask!: ETaskPromise<void>;

  /**
   * Updates the current entity context for the search result
   * @param {DataModelName} newEntity - new entity context for our page
   */
  @action
  onUpdateCurrentEntity(newEntity: DataModelName): void {
    this.onChangeEntity(newEntity);
  }
}
