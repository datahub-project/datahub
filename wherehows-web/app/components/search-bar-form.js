import Component from '@ember/component';
import { computed, set, get } from '@ember/object';
import { inject } from '@ember/service';
import { debounce } from '@ember/runloop';

/**
 * Number of milliseconds to wait before triggering a request for keywords
 * @type {Number}
 */
const keyPressDelay = 180;

export default Component.extend({
  /**
   * Service to retrieve type ahead keywords for a dataset
   * @type {Ember.Service}
   */
  keywords: inject('search-keywords'),

  // Keywords and search Category filter
  currentFilter: 'datasets',

  tagName: 'form',

  elementId: 'global-search-form',

  search: '',

  /**
   * Based on the currentFilter returns a list of options
   *   for the dynamic-link component
   */
  filterOptions: computed('currentFilter', function() {
    const currentFilter = get(this, 'currentFilter');
    return ['datasets', 'metrics', 'flows'].map(filter => ({
      title: filter,
      text: filter,
      action: `filter${filter.capitalize()}`,
      activeWhen: filter === currentFilter
    }));
  }),

  /**
   * Based on the currentFilter returns placeholder text
   */
  placeholder: computed('currentFilter', function() {
    return `Search ${get(this, 'currentFilter')} by keywords... e.g. pagekey`;
  }),

  /**
   * Creates an instance function that can be referenced by key. Acts as a proxy to the queryResolver
   * function which is a new function on each invocation. This allows the debouncing function to retain
   * a reference to the same function for multiple invocations.
   * @return {*}
   */
  debouncedResolver() {
    const queryResolver = get(this, 'keywords.apiResultsFor')(get(this, 'currentFilter'));
    return queryResolver(...arguments);
  },

  actions: {
    /**
     * When a search action is performed, invoke the parent search action with
     *   the user entered search value as keyword and the currentFilter
     *   as category
     */
    search() {
      get(this, 'didSearch')({
        keyword: get(this, 'search'),
        category: get(this, 'currentFilter')
      });
    },

    /**
     * Handles the text input action for potential typeahead matches
     */
    onInput() {
      // Delay invocation until after the given number of ms after each
      //  text input
      debounce(this, this.debouncedResolver, [...arguments], keyPressDelay);
    },

    /**
     * The current dynamic-link implementation sends an action without the option
     *   to pass arguments or using closure actions, hence the need to create
     *   ugly separate individual actions for each filter
     *   A PR will be created to fix this and then the implementation below
     *   refactored to correct this.
     *   TODO: DSS-6760 Create PR to handle action as closure action in dynamic-link
     *     component
     */
    filterDatasets() {
      set(this, 'currentFilter', 'datasets');
    },

    filterMetrics() {
      set(this, 'currentFilter', 'metrics');
    },

    filterFlows() {
      set(this, 'currentFilter', 'flows');
    }
  }
});
