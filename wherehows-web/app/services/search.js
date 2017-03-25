import Ember from 'ember';
import {
  encode
} from 'wherehows-web/utils/encode-decode-uri-component-with-space';

const {
  Service,
  getOwner,
  isBlank
} = Ember;

export default Service.extend({
  /**
   * Transition to the search route including search keyword as query parameter
   * @param {Object} args a map of query parameters to values, including keyword
   * @prop {String|*} args.keyword the string to search for
   * @returns {void|Transition|EmberStates.Transition}
   */
  showSearchResults(args) {
    let { keyword } = args;

    // Transition to search route only if value is not null or void
    if (!isBlank(keyword)) {
      // Lookup application Route on ApplicationInstance
      const applicationRoute = getOwner(this).lookup('route:application');
      keyword = encode(keyword);

      return applicationRoute.transitionTo('search', {
        queryParams: { keyword }
      });
    }
  }
});
