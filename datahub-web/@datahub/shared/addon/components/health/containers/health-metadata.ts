import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/health/containers/health-metadata';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { task, TaskInstance, Task } from 'ember-concurrency';
import { getOrRecalculateHealth } from '@datahub/shared/api/health';
import { set, action } from '@ember/object';
import Notifications from '@datahub/utils/services/notifications';
import { inject as service } from '@ember/service';
import { NotificationEvent } from '@datahub/utils/constants/notifications';
import DataModelsService from '@datahub/data-models/services/data-models';
import { DataModelName } from '@datahub/data-models/constants/entity';
import { tagName, layout } from '@ember-decorators/component';
import { noop, Dictionary } from 'lodash';
import RouterService from '@ember/routing/router-service';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';
import { computed } from '@ember/object';

/**
 * Container component provides the data management for Health Metadata across all supported entities
 * @export
 * @class HealthContainersHealthMetadata
 * @extends {Component}
 */
@tagName('')
@layout(template)
@containerDataSource<HealthContainersHealthMetadata>('getOrRecalculateEntityHealthTask', ['urn', 'entityName'])
export default class HealthContainersHealthMetadata extends Component {
  /**
   * URN identifier for the entity to request Health metadata for
   */
  urn?: string;

  /**
   * The name of the entity for which this component instance should manage Health metadata
   */
  entityName?: DataModelName;

  /**
   * Nullable reference to the Health metadata for the entity with the urn specified on the container
   * Default value is null prior to reification
   */
  health: Com.Linkedin.Common.Health | null = null;

  /**
   * Reference to the app notification service. Users are notified if there are issues with getting the Health metadata
   */
  @service
  notifications!: Notifications;

  /**
   * Reference to the application router instance. Fulfills the user request to transition to an app route for a related Health Validator
   * @type {RouterService}
   */
  @service
  router!: RouterService;

  /**
   * The DataModels service is used to resolve the reference to the DataModelEntity class
   */
  @service
  dataModels!: DataModelsService;

  /**
   * The configurator service is used to reference properties such as health wiki link
   */
  @service
  configurator!: IConfigurator;

  /**
   * Help resource link used by the health components for tooltip links
   */
  get metadataHealthHelp(): string {
    return this.configurator.getConfig('wikiLinks', {
      useDefault: true,
      default: {
        metadataHealth: '#'
      }
    }).metadataHealth;
  }

  /**
   * Flag indicating the current active page route is the Health score route
   * Used by the Health Insight for rendering an option to navigate to the health tab or not
   * @readonly
   */
  @computed('router.currentURL')
  get isViewingHealth(): boolean {
    const { urn, router } = this;

    return Boolean(urn && router.isActive(router.currentRouteName, urn, 'health'));
  }

  /**
   * Container data task reifies the entity with the urn specified on the container if a value is provided along with
   * a matching entity name (identifier)
   * If performed with a true flag, the recalculation api is used instead
   */
  @(task<Promise<Com.Linkedin.Common.Health>, boolean>(function*(
    this: HealthContainersHealthMetadata,
    recalculate = false
  ): IterableIterator<Promise<Com.Linkedin.Common.Health>> {
    const { urn, notifications, entityName } = this;
    let health: HealthContainersHealthMetadata['health'] = null;

    try {
      if (urn && entityName) {
        health = ((yield getOrRecalculateHealth(
          this.dataModels.getModel(entityName),
          urn,
          recalculate
        )) as unknown) as Com.Linkedin.Common.Health;
      }
    } catch (e) {
      notifications.notify({
        type: NotificationEvent.info,
        content: `An error occurred while attempting to get Health Metadata: ${e}`
      });
    } finally {
      // Always set the health value
      set(this, 'health', health);
    }
  }).restartable())
  getOrRecalculateEntityHealthTask!: Task<
    Promise<Com.Linkedin.Common.Health>,
    (isRecalculation?: boolean) => TaskInstance<Promise<Com.Linkedin.Common.Health>>
  >;

  /**
   * Handles user interaction with CTAs for each Health Validator / Factor
   * @param {Com.Linkedin.Common.HealthValidation} validation the related Health Validation object
   */
  @action
  onHealthFactorAction(validation: Com.Linkedin.Common.HealthValidation): void {
    const actions: Dictionary<Function | undefined> = {
      Ownership: this.onViewOwnership
    };
    const validatorAction = actions[validation.validator] || noop;

    try {
      validatorAction();
    } catch (e) {
      this.notifications.notify({
        content: `We could not complete your requested action. ${e}`,
        type: NotificationEvent.error
      });
    }
  }

  /**
   * Transitions to application page to the ownership tab for the related entity
   */
  @action
  onViewOwnership(): void {
    const { urn, router } = this;

    if (urn) {
      router.transitionTo(router.currentRouteName, urn, 'ownership');
    }
  }

  /**
   * Transitions to the health tab for the current entity on invocation
   */
  @action
  onViewHealth(): void {
    const { urn, router } = this;
    if (urn) {
      router.transitionTo(router.currentRouteName, urn, 'health');
    }
  }

  /**
   * Performs the e-c task to recalculate the Health Score value by supplying the true flag when invoked
   */
  @action
  onRecalculateHealthScore(): void {
    this.getOrRecalculateEntityHealthTask.perform(true);
  }
}
