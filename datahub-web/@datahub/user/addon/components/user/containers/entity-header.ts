import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/user/containers/entity-header';
import { layout } from '@ember-decorators/component';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { task } from 'ember-concurrency';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { IPersonEntityEditableProperties } from '@datahub/data-models/types/entity/person/props';

/**
 * The UserContainersEntityHeader component is used to connect our PersonEntity and get all the
 * data necessary to populate an entity header, as well as provide an interface to post back an
 * update to said header
 */
@layout(template)
@containerDataSource<UserContainersEntityHeader>('getContainerDataTask', ['entity'])
export default class UserContainersEntityHeader extends Component {
  /**
   * Underlying person entity that provides context for this container component
   */
  entity?: PersonEntity;

  /**
   * Calls the person entity model to get the data necessary to populate the entity header
   * component on the person entity profile page
   */
  @(task(function*(this: UserContainersEntityHeader): IterableIterator<Promise<void>> {
    const { entity } = this;

    if (entity) {
      // Placeholder for entity api function tasks
    }
  }).restartable())
  getContainerDataTask!: ETaskPromise<void>;

  /**
   * If a person is able to edit their profile, then this function will make the actual post
   * request to do so
   */
  @(task(function*(
    this: UserContainersEntityHeader,
    value: IPersonEntityEditableProperties
  ): IterableIterator<Promise<PersonEntity | void>> {
    const { entity } = this;

    if (entity && entity.updateEditableProperties) {
      yield entity.updateEditableProperties(value);
      yield entity.retrieveAndSetEntityData();
    }
  }).drop())
  updateEditableProfileTask!: ETaskPromise<void, IPersonEntityEditableProperties>;
}
