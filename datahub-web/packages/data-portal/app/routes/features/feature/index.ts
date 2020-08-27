import Route from '@ember/routing/route';
import { FeatureEntity } from '@datahub/data-models/entity/feature/feature-entity';
import Transition from '@ember/routing/-private/transition';

/**
 * Index route for a specific feature should transition to a default tab for Features
 * @export
 * @class FeaturesFeatureIndex
 * @extends {Route}
 */
export default class FeaturesFeatureIndex extends Route {
  /**
   * On navigation to the index route, transition to defaultTab for FeatureEntity
   * @returns {EmberTransition}
   * @memberof FeaturesFeatureIndex
   */
  redirect(): Transition | void {
    const featureEntityPageRenderProps = FeatureEntity.renderProps.entityPage;
    if (featureEntityPageRenderProps) {
      return this.replaceWith('features.feature.tab', featureEntityPageRenderProps.defaultTab);
    }
  }
}
