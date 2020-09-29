import Component from '@ember/component';
import RouterService from '@ember/routing/router-service';
import { inject as service } from '@ember/service';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/entity-page/entity-base-container';
import { action, set, computed } from '@ember/object';
import { layout, tagName } from '@ember-decorators/component';
import { task } from 'ember-concurrency';
import { ETask } from '@datahub/utils/types/concurrency';
import { IEntityContainer } from '@datahub/shared/types/entity-page/containers';
import DataModelsService from '@datahub/data-models/services/data-models';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { BaseEntity } from '@datahub/data-models/entity/base-entity';
import { containerDataSource } from '@datahub/utils/api/data-source';
import {
  IEntityRenderPropsEntityPage,
  ITabProperties
} from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { assertComponentPropertyNotUndefined } from '@datahub/utils/decorators/assert';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';
import { returnDefaultIfNotFound } from '@datahub/utils/api/fetcher';

/**
 * Class for a basic data model entity tagless container component.
 * Preferred usage is composition over inheritance, i.e.
 * if other functionality is required, a secondary component may be used within the context of EntityBaseContainer.
    <EntityPage::EntityBaseContainer
      @urn={{@urn}}
      @entityClass={{@entityClass}}
      @currentTab={{@tabSelected}} as |baseContainer|
    >
      <NestedCustomContainer @entity={{baseContainer.entity}} as |nestedContainer|>
        {{! nested presentational components}}
      </NestedCustomContainer>
    </EntityPage::EntityBaseContainer>
 *
 * However, is extensible to provide per entity customization in certain cases.
 * Conforms to the interface defined in IEntityContainer providing the basic members needed for an entity container component
 * @export
 * @class EntityBaseContainer
 * @extends {Component}
 * @implements {IEntityContainer<E>}
 * @template E the DataModelEntity type this container is responsible for, found in the DataModelEntity type
 */
@tagName('')
@layout(template)
@containerDataSource<EntityBaseContainer<BaseEntity<{}>>>('reifyEntityTask', ['urn'])
export default class EntityBaseContainer<E extends DataModelEntityInstance> extends Component
  implements IEntityContainer<E> {
  /**
   * URN identifier for the related data entity specified by EntityBaseContainer<E>['entity']
   */
  @assertComponentPropertyNotUndefined
  urn!: string;

  /**
   * Reference to the associated and reified instance of the supplied entityClass
   */
  entity?: E;

  /**
   * The class (DataModel) for the data model entity instance
   */
  @assertComponentPropertyNotUndefined
  entityClass!: ReturnType<DataModelsService['getModel']>;

  /**
   * The currently selected entity page tab
   */
  @assertComponentPropertyNotUndefined
  currentTab!: string;

  /**
   * Error returned when api fetch failed
   */
  error?: Error;

  /**
   * References the injected application / host router service
   */
  @service('router')
  router!: RouterService;

  /**
   * The injected application DataModels service
   */
  @service('data-models')
  dataModels!: DataModelsService;

  /**
   * Configurator service if available
   */
  @service
  configurator?: IConfigurator;

  /**
   * Jit ACL configs
   */
  jitAclConfig = this.configurator?.getConfig('jitAcl');

  /**
   * List of tabs that are available to be rendered via the Entity container component
   * tabs are yielded by the component and can be rendered by the presentational component within the block
   * Tabs are provided to the contextual presentational component to render the list of available Tabs
   */
  @computed('entity')
  get tabs(): Array<ITabProperties> {
    const { entityClass, entity } = this;
    const emptyTabProperties: Array<ITabProperties> = [];

    if (entityClass && entityClass.renderProps && entityClass.renderProps.entityPage && entity) {
      const entityPage: IEntityRenderPropsEntityPage<E> = entityClass.renderProps.entityPage;
      return typeof entityPage.tabProperties === 'function'
        ? entityPage.tabProperties(entity)
        : entityPage.tabProperties;
    }

    return emptyTabProperties;
  }

  /**
   * Container data task to reify the container's entity instance on component DOM insertion or container dependent key update
   */
  @(task(function*(this: EntityBaseContainer<E>): IterableIterator<Promise<E | undefined>> {
    const { urn, dataModels, entityClass } = this;
    let entity: EntityBaseContainer<E>['entity'];

    if (urn && entityClass) {
      try {
        set(this, 'error', undefined);
        entity = yield returnDefaultIfNotFound(
          (dataModels.createInstance(entityClass.displayName, urn) as unknown) as Promise<E | undefined>,
          undefined
        );
      } catch (e) {
        set(this, 'error', e);
        throw e;
      } finally {
        // Always set or reset the entity to the resolved or default value on each try
        // For example, if the urn changes, but an exception is thrown, ensure the previous entity is replaced with the default
        set(this, 'entity', entity);
      }
    }
  }).restartable())
  reifyEntityTask!: ETask<E>;

  /**
   * Handles the tab selection action from the UI element for the user selected navigation tab
   * @param {Tab} tabSelected the unique tab identifier selected by the user
   */
  @action
  tabSelectionDidChange(tabSelected: string): void {
    const { router, urn } = this;

    // Perform transition if the currentTab is not the one currently selected
    if (urn && this.currentTab !== tabSelected) {
      router.transitionTo(router.currentRouteName, urn, tabSelected);
    }
  }
}
