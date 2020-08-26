import Component from '@ember/component';
import { setProperties, set } from '@ember/object';
import { computed } from '@ember/object';
import { alias, or } from '@ember/object/computed';
import { inject as service } from '@ember/service';
import { task } from 'ember-concurrency';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { IBrowsePath } from '@datahub/data-models/types/entity/shared';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import DataModelsService from '@datahub/data-models/services/data-models';
import { DataModelName } from '@datahub/data-models/constants/entity/index';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/browser/containers/entity-categories';
import { layout } from '@ember-decorators/component';

/**
 * Describes the properties that are returned from the BrowseEntityCategory route's model hook
 * if the current entity is implemented using the BaseEntity data model class, then properties
 * are augmented with attributes to enable proper rendering
 * @interface IBrowseEntityModel
 */
export interface IBrowseEntityModel {
  // Entity display name used in breadcrumbs component
  displayName?: string;
  // category and prefix string values
  segments?: Array<string>;
  entity: DataModelName | typeof MockEntity.displayName;
  page: number;
  size: number;
  // References the DataModelEntity for the current entity being browsed
  showHierarchySearch?: boolean;
}

/**
 * Defines the container component to fetch nodes for a given entity constrained by category, or prefix, etc
 * For example fetch the elements for a Ump metric where the parameter is a bucket
 * @export
 * @class EntityCategoriesContainer
 * @extends {Component}
 */
@layout(template)
@containerDataSource<EntityCategoriesContainer>('getEntityCategoriesNodesTask', ['params'])
export default class EntityCategoriesContainer extends Component {
  /**
   * Route / url properties to redirect to data system
   */
  params?: IBrowseEntityModel;

  /**
   * Contains the information about the actual path like counts, folders, entities
   */
  browsePath?: IBrowsePath;

  /**
   * References the current DataModelEntity class if applicable
   */
  entityType?: DataModelEntity;

  /**
   * Data Models Service will be used to load the right model for an entity type to fetch
   * the browsing path.
   */
  @service('data-models')
  dataModels!: DataModelsService;

  /**
   * Total count of entities inside groups
   */
  @computed('browsePath')
  get totalNumEntities(): number {
    const { browsePath } = this;
    const { totalNumEntities = 0 } = browsePath || {};
    return totalNumEntities;
  }

  /**
   * Total entities in this group
   */
  @computed('browsePath')
  get entitiesPaginationCount(): number {
    const { browsePath } = this;
    const { entitiesPaginationCount = 0 } = browsePath || {};
    return entitiesPaginationCount;
  }
  /**
   * Flag indicated if the browser component to perform a search within a category should be visible in the ui or hidden
   */
  @alias('params.showHierarchySearch')
  showSearchWithinHierarchy?: boolean;

  /**
   * should show category as tiles instead of list of items
   */
  showAsTiles = false;

  /**
   * Loading state for the entities
   */
  isLoadingEntitiesList = false;

  /**
   * Loading state for the groups
   */
  isLoadingGroupsList = false;

  /**
   * Since there are two lists, we will show the spinner until
   * both lists are done
   */
  @or('isLoadingGroupsList', 'isLoadingEntitiesList')
  isLoadingList!: boolean;

  /**
   * Current page for element pagination
   */
  page = 0;

  /**
   * Number of elements to fetch per page
   */
  count = 100;

  /**
   * Invokes 'initialLoad' with async syntax instead of relying on generators since
   * ember-concurrency syntax is not settle and can be problematic.
   */
  @(task(function*(this: EntityCategoriesContainer): IterableIterator<Promise<void>> {
    yield this.initialLoad();
  }).restartable())
  getEntityCategoriesNodesTask!: ETaskPromise<void>;

  /**
   * Initial load to request entities or categories based on a prefix or category if available
   */
  async initialLoad(): Promise<void> {
    const { segments = [], entity } = this.params || {};
    const entityType = this.dataModels.getModel(entity as DataModelName);
    setProperties(this, {
      entityType,
      // NOTE: for now there is no straight forward way to specify this in the render props,
      // since the future of this is unknown, hardcoding this logic here. Feel free to refractor
      // this into render props.
      showAsTiles: (!segments || segments.length === 0) && entity === DatasetEntity.displayName,
      isLoadingEntitiesList: true,
      isLoadingGroupsList: true,
      page: 0
    });
    set(this, 'browsePath', await this.fetchBrowsePath());
  }

  /**
   * Will fetch and return data for current page
   */
  async fetchBrowsePath(): Promise<EntityCategoriesContainer['browsePath']> {
    const { segments = [] } = this.params || {};
    const { page, count, entityType } = this;

    return entityType && (await entityType.readCategories(segments, { page, count }));
  }

  /**
   * Given page and number of items per page, it will calculate
   * the amount of items that we have fetched so far.
   */
  get numFetchedSoFar(): number {
    const { page, count } = this;
    return (page + 1) * count;
  }

  /**
   * Whether there is more items to fetch from BE or not
   */
  get hasMoreItems(): boolean {
    const { entitiesPaginationCount, numFetchedSoFar } = this;
    return numFetchedSoFar < entitiesPaginationCount;
  }

  /**
   * Closure action for big-list onFinished for entity list
   *
   * it will return additional items to load into the list
   */
  async onFinishedLoadingEntities(): Promise<IBrowsePath['entities']> {
    const { hasMoreItems, page } = this;
    if (hasMoreItems) {
      set(this, 'page', page + 1);
      const browsePath = await this.fetchBrowsePath();
      if (browsePath && browsePath.entities.length > 0) {
        return browsePath.entities;
      }
    }

    set(this, 'isLoadingEntitiesList', false);
    return [];
  }

  /**
   * Closure action for big-list onFinished for entity list
   */
  onFinishedLoadingGroups(): void {
    set(this, 'isLoadingGroupsList', false);
  }

  /**
   * Closure action for big-list onFinished for tiles (no entities should exist in this list)
   */
  onFinishedLoadingTiles(): void {
    setProperties(this, {
      isLoadingEntitiesList: false,
      isLoadingGroupsList: false
    });
  }
}
