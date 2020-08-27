import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/configurable/containers/entity-feature-configs';
import { task } from 'ember-concurrency';
import { layout } from '@ember-decorators/component';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { readEntityFeatureConfigs } from '@datahub/shared/api/entity-configs';
import { alias } from '@ember/object/computed';
import { set } from '@ember/object';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';

/**
 * This container is the data source for requesting entity feature configs
 * Renders the yielded components if there are configs for the provided entity @urn and config @target
 *
 * TODO META-11238: Separate entity feature configs container with configurable feature wrapper
 * This container should not contain render logic after being genericized
 */
@layout(template)
@containerDataSource<EntityFeatureConfigsContainer>('getContainerDataTask', ['entity', 'targetFeature'])
export default class EntityFeatureConfigsContainer extends Component {
  /**
   * The entity providing context for the feature configs
   */
  entity?: DataModelEntityInstance;

  /**
   * The urn of the provided entity used for requesting configs specific to that entity
   */
  @alias('entity.urn')
  urn?: string;

  /**
   * The target feature for which this container is requesting entity configs for
   * TODO META-11234: Allow for entity feature configs container to batch for multiple targets
   */
  targetFeature = '';

  /**
   * The entity configs used for rendering configurable feature components
   * TODO META-11234: Allow for entity feature configs container to batch for multiple targets
   * Configs should be a record of configs when there are multiple targets however the current midtier implementation
   * only fetches for an appworx-deprecation flag
   */
  configs = false;

  /**
   * Parent container task to get all data for the container component
   */
  @task(function*(this: EntityFeatureConfigsContainer): IterableIterator<Promise<boolean>> {
    if (this.urn && this.targetFeature) {
      const configs: boolean = ((yield readEntityFeatureConfigs(this.urn, this.targetFeature)) as unknown) as boolean;

      set(this, 'configs', configs);
    }
  })
  getContainerDataTask!: ETaskPromise<Promise<boolean>>;
}
