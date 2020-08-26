import Route from '@ember/routing/route';
import Transition from '@ember/routing/-private/transition';

/**
 * The index route on Features is unused, but should transition to the browse entity route for features
 * @export
 * @class FeaturesIndex
 * @extends {Route}
 */
export default class FeaturesIndex extends Route {
  redirect(): Transition {
    // default transition to browse route if user enters through index
    return this.transitionTo('browse.entity', 'features');
  }
}
