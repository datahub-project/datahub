import Component from '@ember/component';
import { setProperties, set } from '@ember/object';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { UnWrapPromise } from 'wherehows-web/typings/generic';
import { IBrowsePath } from '@datahub/data-models/types/entity/shared';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { computed } from '@ember/object';
import { alias, or } from '@ember/object/computed';
import BrowseEntity from 'wherehows-web/routes/browse/entity';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { task } from 'ember-concurrency';
import { getConfig } from 'wherehows-web/services/configurator';
import { BaseEntity } from '@datahub/data-models/entity/base-entity';
import { ETaskPromise } from '@datahub/utils/types/concurrency';

/**
 * Defines the container component to fetch nodes for a given entity constrained by category, or prefix, etc
 * For example fetch the elements for a Ump metric where the parameter is a bucket
 * @export
 * @class EntityCategoriesContainer
 * @extends {Component}
 */
@containerDataSource('getEntityCategoriesNodesTask', ['params'])
export default class EntityCategoriesContainer extends Component {
  /**
   * Route / url properties to redirect to data system
   */
  params: UnWrapPromise<ReturnType<BrowseEntity['model']>>;

  /**
   * Contains the information about the actual path like counts, folders, entities
   */
  browsePath?: IBrowsePath;

  /**
   * Flag to say whether we are using the new api for datasets
   */
  useNewBrowseDataset: boolean = getConfig('useNewBrowseDataset');

  /**
   * References the current DataModelEntity class if applicable
   */
  entityType?: DataModelEntity;

  /**
   * Total count of entities inside groups
   */
  @computed('browsePath')
  get totalCount(): number {
    const { browsePath } = this;
    const { count = 0 } = browsePath || {};
    return count;
  }

  /**
   * Flag indicated if the browser component to perform a search within a category should be visible in the ui or hidden
   */
  @alias('params.showHierarchySearch')
  showSearchWithinHierarchy: boolean;

  /**
   * should show category as tiles instead of list of items
   */
  showAsTiles: boolean = false;

  /**
   * Loading state for the entities
   */
  isLoadingEntitiesList: boolean = false;

  /**
   * Loading state for the groups
   */
  isLoadingGroupsList: boolean = false;

  /**
   * Since there are two lists, we will show the spinner until
   * both lists are done
   */
  @or('isLoadingGroupsList', 'isLoadingEntitiesList')
  isLoadingList: boolean;

  /**
   * Task to request entities or categories based on a prefix or category if available
   */
  @(task(function*(this: EntityCategoriesContainer): IterableIterator<Promise<IBrowsePath>> {
    const { segments = [], entity } = this.params;
    const { useNewBrowseDataset } = this;
    const entityType = DataModelEntity[entity];

    let browsePath: EntityCategoriesContainer['browsePath'];

    try {
      if (useNewBrowseDataset && entity === DatasetEntity.displayName) {
        // Since datamodels cant read config, we are doing the flag guard here
        // once the code is ready, then we just need to remove the implementation from datasets
        browsePath = yield BaseEntity.readCategories.apply(entityType, segments);
      } else {
        browsePath = yield entityType.readCategories(...segments);
      }
    } finally {
      setProperties(this, {
        browsePath,
        entityType,
        // NOTE: for now there is no straight forward way to specify this in the render props,
        // since the future of this is unknown, hardcoding this logic here. Feel free to refractor
        // this into render props.
        showAsTiles: (!segments || segments.length === 0) && entity === DatasetEntity.displayName,
        isLoadingEntitiesList: true,
        isLoadingGroupsList: true
      });
    }
  }).restartable())
  getEntityCategoriesNodesTask!: ETaskPromise<IBrowsePath>;
  /**
   * Closure action for big-list onFinished for entity list
   *
   * TODO META-9198 complete server side pagination
   */
  onFinishedLoadingEntities(): void {
    set(this, 'isLoadingEntitiesList', false);
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
