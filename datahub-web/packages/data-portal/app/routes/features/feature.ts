import Route from '@ember/routing/route';

/**
 * Defines the attributes for the model hook of the FeaturesFeature route
 * @interface IFeatureRouteParams
 */
interface IFeatureRouteParams {
  // urn identifier for the Feature
  feature_urn: string;
}

/**
 * Route class for a Feature Entity instance
 * @export
 * @class FeaturesFeature
 * @extends {Route}
 */
export default class FeaturesFeature extends Route {
  /**
   * Receives the requested feature_urn and returns same to be used by container
   * @param {IFeatureRouteParams} { feature_urn: urn }
   * @returns {{ urn: string }}}
   * @memberof FeaturesFeature
   */
  // eslint-disable-next-line @typescript-eslint/camelcase
  model({ feature_urn: urn }: IFeatureRouteParams): { urn: string } {
    return {
      urn
    };
  }
}
