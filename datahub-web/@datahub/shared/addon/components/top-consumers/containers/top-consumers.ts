import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/top-consumers/containers/top-consumers';
import { layout, classNames, tagName } from '@ember-decorators/component';
import { alias, map } from '@ember/object/computed';
import { setProperties } from '@ember/object';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { task } from 'ember-concurrency';
import { IDynamicLinkParams } from 'dynamic-link/components/dynamic-link';
import { DataModelName, DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { readTopConsumersForEntity } from '@datahub/shared/api/top-consumers';
import { getGridGroupFromUrn } from '@datahub/data-models/utils/get-group-from-urn';

/**
 * This container is the data source for the top consumers aspect for an entity
 */
@classNames('top-consumers')
@layout(template)
@tagName('')
@containerDataSource<TopConsumersContainer>('getTopConsumersTask', ['urn', 'entityType'])
export default class TopConsumersContainer extends Component {
  /**
   * An external entity that gives context to this container for fetching the appropriate top consumers insight
   */
  entity?: DataModelEntityInstance;

  /**
   * A list of people urns for the top users that consume the entity
   */
  topUserUrns: Array<string> = [];

  /**
   * A list of group urns for the top groups that consume the entity
   */
  topGroupUrns: Array<string> = [];

  /**
   * The prefix for grid group primary entity page
   * TODO META-11471: Refactor top consumers for internal vs external compatibility
   */
  gridGroupUrlPrefix = 'https://www.grid.linkedin.com/self_service/#/account/';

  /**
   * The urn of the context entity for top consumers
   */
  @alias('entity.urn')
  urn = '';

  /**
   * The entity type of the context entity for top consumers
   */
  @alias('entity.displayName')
  entityType?: DataModelName;

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
   * The task used to fetch the top consumers aspect and provides context to the container
   */
  @task(function*(
    this: TopConsumersContainer
  ): IterableIterator<Promise<Com.Linkedin.Common.EntityTopUsage> | Promise<Array<PersonEntity>>> {
    const { entityType, urn } = this;

    let topUserUrns: Array<string> = [];
    let topGroupUrns: Array<string> = [];

    if (entityType && urn) {
      try {
        const { mostFrequentUsers, mostFrequentGroups } = ((yield readTopConsumersForEntity(
          entityType,
          urn
        )) as unknown) as Com.Linkedin.Common.EntityTopUsage;

        topUserUrns = mostFrequentUsers.map((user): string => (user.identity as { corpUser?: string }).corpUser || '');
        topGroupUrns = mostFrequentGroups.map(
          (group): string =>
            (group.identity as { gridGroup?: string }).gridGroup ||
            (group.identity as { corpGroup?: string }).corpGroup ||
            ''
        );
      } finally {
        setProperties(this, {
          topUserUrns,
          topGroupUrns
        });
      }
    }
  })
  getTopConsumersTask!: ETaskPromise<Com.Linkedin.Common.EntityTopUsage | PersonEntity>;
}
