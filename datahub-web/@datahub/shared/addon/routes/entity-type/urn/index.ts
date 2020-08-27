import Route from '@ember/routing/route';
import { EntityTypeUrnModel } from '@datahub/shared/routes/entity-type/urn';

/**
 * Route check the default tab for the entity page and redirect to it when no tab is defined in the route
 */
export default class EntityTypeUrnIndex extends Route {
  redirect(): void {
    const model = this.modelFor('entity-type.urn') as EntityTypeUrnModel;
    const defaultTab = model?.entityClass?.renderProps?.entityPage?.defaultTab;

    if (defaultTab) {
      const defaultTabId = defaultTab instanceof Function ? defaultTab(model.urn) : defaultTab;
      this.transitionTo('entity-type.urn.tab', model.urn, defaultTabId);
    }
  }
}
