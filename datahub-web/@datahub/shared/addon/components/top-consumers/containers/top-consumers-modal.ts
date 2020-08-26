import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/top-consumers/containers/top-consumers-modal';
import { layout, tagName } from '@ember-decorators/component';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { inject as service } from '@ember/service';
import DataModelsService from '@datahub/data-models/services/data-models';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { task } from 'ember-concurrency';
import { IGridGroupEntity } from '@datahub/shared/types/grid-group';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { getGridGroupFromUrn } from '@datahub/data-models/utils/get-group-from-urn';
import { setProperties } from '@ember/object';

/**
 * The container data source for the top consumers modal
 */
@layout(template)
@tagName('')
@containerDataSource<TopConsumersModalContainer>('instantiateTopConsumersTask', ['topUserUrns', 'topGroupUrns'])
export default class TopConsumersModalContainer extends Component {
  /**
   * Injection of the application's data modeling service
   */
  @service('data-models')
  dataModels!: DataModelsService;

  /**
   * Externally supplied list of people urns for the top users
   */
  topUserUrns: Array<string> = [];

  /**
   * Externally supplied list of group urns of the top groups
   */
  topGroupUrns: Array<string> = [];

  /**
   * A list of person instances for the top users created from the task
   */
  topUsers: Array<PersonEntity> = [];

  /**
   * A list of group instances for the top groups created from the task
   */
  topGroups: Array<IGridGroupEntity> = [];

  /**
   * The prefix for grid group primary entity page
   * TODO META-11471: Refactor top consumers for internal vs external compatibility
   */
  gridGroupUrlPrefix = 'https://www.grid.linkedin.com/self_service/#/account/';

  /**
   * The task used to instantiate all entity instances of the top consumers
   */
  @task(function*(this: TopConsumersModalContainer): IterableIterator<Promise<Array<PersonEntity>>> {
    const { topUserUrns, topGroupUrns, dataModels, gridGroupUrlPrefix } = this;

    let topUsers: Array<PersonEntity> = [];
    let topGroups: Array<IGridGroupEntity> = [];

    try {
      const pendingPersonInstances = topUserUrns.map(
        (userUrn): Promise<PersonEntity> => dataModels.createInstance(PersonEntity.displayName, userUrn)
      );

      topUsers = ((yield Promise.all(pendingPersonInstances)) as unknown) as Array<PersonEntity>;
      topGroups = topGroupUrns.map(
        (groupUrn): IGridGroupEntity => {
          const name = getGridGroupFromUrn(groupUrn);

          return {
            urn: groupUrn,
            name,
            link: `${gridGroupUrlPrefix}${name}`
          };
        }
      );
    } finally {
      setProperties(this, {
        topUsers,
        topGroups
      });
    }
  })
  instantiateTopConsumersTask!: ETaskPromise<PersonEntity>;
}
