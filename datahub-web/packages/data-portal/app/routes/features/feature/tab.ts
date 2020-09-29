import Route from '@ember/routing/route';
import FeaturesFeature from 'datahub-web/routes/features/feature';
import { UnWrapPromise } from '@datahub/utils/types/async';
import CurrentUser from '@datahub/shared/services/current-user';
import { inject as service } from '@ember/service';
import { alias } from '@ember/object/computed';
import { getConfig } from '@datahub/shared/services/configurator';
import { IAppConfig } from '@datahub/shared/types/configurator/configurator';

/**
 * Defines the parameters expected on the route
 * @interface IFeaturesFeatureTabRouteParams
 */
interface IFeaturesFeatureTabRouteParams {
  tab_selected?: string; // eslint-disable-line @typescript-eslint/camelcase
}

/**
 * Route model return value interface
 * @interface IFeaturesFeatureTabRouteModel
 */
interface IFeaturesFeatureTabRouteModel {
  // Urn for the Feature Entity
  urn: string;
  // userName for the currently logged in user
  userName: string;
  // Current tab selection for the entity page
  tab_selected?: string;
  // Host application config properties that addon's may use to read current application config state
  hostConfig: IAppConfig;
}

/**
 * Defines the class for the features.feature.tab route
 * @export
 * @class FeaturesFeatureTab
 * @extends {Route}
 */
export default class FeaturesFeatureTab extends Route {
  /**
   * References the CurrentUser service
   * @type {CurrentUser}
   * @memberof FeaturesFeatureTab
   */
  @service('current-user')
  currentUser: CurrentUser;

  /**
   * Aliases the ldap username of the currently logged in user
   * @type {string}
   * @memberof FeaturesFeatureTab
   */
  @alias('currentUser.entity.username')
  userName: string;

  /**
   * Resolves with properties for setting up the FeatureRoute, data needs are handled in the container component
   * @param {IFeaturesFeatureTabRouteParams} { tab_selected }
   * @returns
   * @memberof FeaturesFeatureTab
   */
  // eslint-disable-next-line @typescript-eslint/camelcase
  model({ tab_selected }: IFeaturesFeatureTabRouteParams): IFeaturesFeatureTabRouteModel {
    const { urn } = this.modelFor('features.feature') as UnWrapPromise<ReturnType<FeaturesFeature['model']>>;
    const hostConfig: IAppConfig = getConfig<undefined>();
    const { userName } = this;

    return {
      urn,
      userName,
      hostConfig,
      tab_selected // eslint-disable-line @typescript-eslint/camelcase
    };
  }
}
