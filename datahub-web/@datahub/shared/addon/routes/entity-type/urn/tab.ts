import Route from '@ember/routing/route';
import { EntityTypeUrnModel } from '@datahub/shared/routes/entity-type/urn';
import { decodeUrn } from '@datahub/utils/validators/urn';
import { refreshModelForQueryParams } from '@datahub/utils/routes/refresh-model-for-query-params';
import { ProtocolController } from '@datahub/utils/controllers/protocol';

/**
 * Types for the routes model. It will use parent and append tabSelected
 */
export type EntityTypeUrnTabModel = EntityTypeUrnModel & {
  tabSelected: string;
  requestJitUrns: Array<string>;
};

/**
 * Types for the route params
 */
interface IEntityTypeUrnParams {
  tab_selected: string;
  request_jit_urns: Array<string>;
}

/**
 * Route will capture the string tabSelected and passed to the component defined in the renderProps for
 * the entity page.
 */
export default class EntityTypeUrnTab extends Route {
  // TODO META-11672 avoid hardcoding dataset specific params in the entity route
  queryParams = refreshModelForQueryParams(['field_filter', 'request_jit_urns']);
  /**
   * Will capture tabSelected return it with the parent model.
   */
  model({
    tab_selected: tabSelected,
    request_jit_urns: requestJitUrns
  }: IEntityTypeUrnParams): EntityTypeUrnTabModel | void {
    const model = this.modelFor('entity-type.urn') as EntityTypeUrnModel;

    // Decodes urns in the top level query params to pass down for legibility and urn format validation
    requestJitUrns = (requestJitUrns || []).map((urn: string): string => decodeUrn(urn));

    if (model) {
      return {
        ...model,
        tabSelected,
        requestJitUrns
      };
    }
  }

  resetController(controller: ProtocolController, isExiting: boolean): void {
    // TODO META-10699: Remove query params from URL on default values for bulk jit acl request
    if (isExiting) {
      controller.resetProtocol();
    }
  }
}
