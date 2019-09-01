import Component from '@ember/component';
import { set } from '@ember/object';
import { Task, task } from 'ember-concurrency';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { tagName } from '@ember-decorators/component';
import { DataModelEntity, DataModelName } from '@datahub/data-models/constants/entity';

// TODO META-8863 remove once dataset is migrated
@tagName('')
@containerDataSource('getEntityCountTask', ['entity', 'category'])
export default class EntityCategoryContainer extends Component {
  /**
   * Name of the entity category container is related to, externally supplied value
   */
  entity!: DataModelName;

  category!: string;

  prefix?: string;

  count: number = 0;

  /**
   * Task to request the data platform's count
   * @type {(Task<Promise<number>, (a?: any) => TaskInstance<Promise<number>>>)}
   */
  @task(function*(this: EntityCategoryContainer): IterableIterator<Promise<number>> {
    const { entity, category } = this;
    const modelEntity: DataModelEntity = DataModelEntity[entity];
    set(this, 'count', yield modelEntity.readCategoriesCount(category));
  })
  getEntityCountTask!: Task<Promise<number>, () => Promise<number>>;
}
