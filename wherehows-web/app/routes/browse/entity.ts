import Route from '@ember/routing/route';
import { setProperties, get } from '@ember/object';
import { inject } from '@ember/service';
import ComputedProperty from '@ember/object/computed';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { refreshModelQueryParams } from 'wherehows-web/utils/helpers/routes';
import BrowseEntityController from 'wherehows-web/controllers/browse/entity';
import Configurator from 'wherehows-web/services/configurator';
import { IAppConfig } from 'wherehows-web/typings/api/configurator/configurator';

const queryParamsKeys: Array<keyof IBrowserRouteParams> = ['page', 'prefix', 'platform', 'size'];

/**
 * Describes the route parameter interface for the browser route
 * @export
 * @interface IBrowserRouteParams
 */
export interface IBrowserRouteParams {
  entity: string | 'datasets';
  page: number;
  size: number;
  platform: string;
  prefix: string | void;
}

export default class BrowseEntity extends Route.extend(AuthenticatedRouteMixin, {
  resetController(controller: BrowseEntityController, isExiting: boolean) {
    if (isExiting) {
      // clear out sticky params
      setProperties(controller, { prefix: '', platform: '' });
    }
  }
}) {
  queryParams = refreshModelQueryParams(queryParamsKeys);

  /**
   * References the application's Configurator service
   * @type {ComputedProperty<Configurator>}
   * @memberof DatasetRoute
   */
  configurator: ComputedProperty<Configurator> = inject();

  setupController(this: BrowseEntity, controller: BrowseEntityController, model: IBrowserRouteParams) {
    const configuratorService = get(this, 'configurator');

    // sets the entity property on the controller in addition to the model
    setProperties(controller, { entity: model.entity, model });

    const shouldShowBrowserRevamp = <boolean>configuratorService.getConfig<IAppConfig['shouldShowBrowserRevamp']>(
      'shouldShowBrowserRevamp'
    );
    setProperties(controller, {
      shouldShowBrowserRevamp
    });
  }

  static model(params: IBrowserRouteParams): IBrowserRouteParams {
    const { entity, platform, page, size, prefix } = params;
    return { entity, platform, page, size, prefix };
  }
}
