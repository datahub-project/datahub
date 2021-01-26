import Component from '@glimmer/component';
import { alias, map } from '@ember/object/computed';
import { IDynamicLinkParams } from 'dynamic-link/components/dynamic-link';
import { DataModelName, DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { getGridGroupFromUrn } from '@datahub/data-models/utils/get-group-from-urn';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { task } from 'ember-concurrency';
import { readTopConsumersForEntity } from '@datahub/shared/api/top-consumers';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { setAspect, hasAspect } from '@datahub/data-models/entity/utils/aspects';

/**
 * This container is the data source for the top consumers aspect for an entity
 */
export default class TopConsumersContainer extends Component<{
  /**
   * An external entity that gives context to this container for fetching the appropriate top consumers insight
   */
  entity?: DataModelEntityInstance;
}> {
  /**
   * The prefix for grid group primary entity page
   * TODO META-11471: Refactor top consumers for internal vs external compatibility
   */
  gridGroupUrlPrefix = 'https://www.grid.linkedin.com/self_service/#/account/';

  /**
   * The urn of the context entity for top consumers
   */
  @alias('args.entity.urn')
  urn = '';

  /**
   * The entity type of the context entity for top consumers
   */
  @alias('args.entity.displayName')
  entityType?: DataModelName;

  /**
   * The aspect of the entity that it is passed in
   */
  @alias('args.entity.entityTopUsage')
  entityTopUsage?: Com.Linkedin.Common.EntityTopUsage;

  /**
   * URNs of top users
   */
  @map('entityTopUsage.mostFrequentUsers', (user): string => user.identity.corpUser || '')
  topUserUrns?: Array<string>;

  /**
   * URNs of top groups
   */
  @map(
    'entityTopUsage.mostFrequentGroups',
    (group): string => group.identity.gridGroup || group.identity.corpGroup || ''
  )
  topGroupUrns?: Array<string>;

  /**
   * A map of link params that opens a new tab to the top group's entity page
   * The top consumers insight consumes this to render links for users to go to a top group's primary entity page (not on DataHub)
   */
  @map('topGroupUrns', function(this: TopConsumersContainer, groupUrn: string): IDynamicLinkParams {
    return {
      href: `${this.gridGroupUrlPrefix}${groupUrn}`,
      target: '_blank',
      text: getGridGroupFromUrn(groupUrn)
    };
  })
  topGroupLinkParams!: Array<IDynamicLinkParams>;

  /**
   * The task used to fetch the top consumers aspect and provides context to the container.
   * The container will only fetch if the aspect is not defined
   */
  @task(function*(
    this: TopConsumersContainer
  ): IterableIterator<Promise<Com.Linkedin.Common.EntityTopUsage> | Promise<Array<PersonEntity>>> {
    const {
      entityType,
      urn,
      args: { entity }
    } = this;

    if (entity && entityType && urn && !hasAspect(entity, 'entityTopUsage')) {
      const entityTopUsage = ((yield readTopConsumersForEntity(
        entityType,
        urn
      )) as unknown) as Com.Linkedin.Common.EntityTopUsage;

      setAspect(entity, 'entityTopUsage', entityTopUsage);
    }
  })
  getTopConsumersTask!: ETaskPromise<Com.Linkedin.Common.EntityTopUsage | PersonEntity>;
}
