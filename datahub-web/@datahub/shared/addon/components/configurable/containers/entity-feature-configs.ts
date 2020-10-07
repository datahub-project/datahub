import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/configurable/containers/entity-feature-configs';
import { task } from 'ember-concurrency';
import { layout } from '@ember-decorators/component';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { readEntityFeatureConfigs } from '@datahub/shared/api/entity-configs';
import { alias } from '@ember/object/computed';
import { set } from '@ember/object';
import { zipObject } from 'lodash';
import { inject as service } from '@ember/service';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';

/**
 * This container is the data source for requesting entity feature configs
 * Renders the yielded components if there are configs for the provided entity @urn and config @target
 *
 * TODO META-11238: Separate entity feature configs container with configurable feature wrapper
 * This container should not contain render logic after being genericized
 */
@layout(template)
@containerDataSource<EntityFeatureConfigsContainer>('getContainerDataTask', ['entity'])
export default class EntityFeatureConfigsContainer extends Component {
  @service
  configurator!: IConfigurator;

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
  get targetFeatures(): Array<string> {
    const { configurator, entity } = this;
    const targetableFeatures = configurator.getConfig('entityFeatureConfigTargets') || {};
    const entityTargetableFeatures = targetableFeatures[entity?.displayName || ''];
    return entityTargetableFeatures || [];
  }

  /**
   * The entity configs used for rendering configurable feature components
   * TODO META-11234: Allow for entity feature configs container to batch for multiple targets
   * Configs should be a record of configs when there are multiple targets however the current midtier implementation
   * only fetches for an appworx-deprecation flag
   */
  configs: Record<string, boolean> = {};

  /**
   * Parent container task to get all data for the container component
   */
  @task(function*(this: EntityFeatureConfigsContainer): IterableIterator<Promise<Array<boolean>>> {
    const { urn, targetFeatures } = this;
    if (urn && this.targetFeatures.length > 0) {
      const configs = ((yield Promise.all(
        targetFeatures.map((target): Promise<boolean> => readEntityFeatureConfigs(urn, target))
      )) as unknown) as Array<boolean>;
      // ['target1', 'target2'] + [true, false] => { target1: true, target2: false }
      const configsMappedToTargets = zipObject(targetFeatures, configs);
      set(this, 'configs', configsMappedToTargets);
    }
  })
  getContainerDataTask!: ETaskPromise<Promise<boolean>>;
}
