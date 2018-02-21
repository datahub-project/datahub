import Route from '@ember/routing/route';
import { setProperties } from '@ember/object';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { refreshModelQueryParams } from 'wherehows-web/utils/helpers/routes';
import EntityController from 'wherehows-web/controllers/browse/entity';

const queryParamsKeys = ['page', 'prefix', 'platform', 'size'];

/**
 * Describes the route parameter interface for the browser route
 * @export
 * @interface IBrowserRouteParams
 */
export interface IBrowserRouteParams {
  entity: 'datasets' | 'metrics' | 'flows';
  page: number;
  size: number;
  platform: string;
  prefix: string;
}

export default class extends Route.extend(AuthenticatedRouteMixin) {
  queryParams = refreshModelQueryParams(queryParamsKeys);

  setupController(controller: EntityController, model: IBrowserRouteParams) {
    // sets the entity property on the controller in addition to the model
    setProperties(controller, { entity: model.entity, model });
  }

  model(params: IBrowserRouteParams): IBrowserRouteParams {
    const { entity, platform, page, size, prefix } = params;
    return { entity, platform, page, size, prefix };
  }
}
