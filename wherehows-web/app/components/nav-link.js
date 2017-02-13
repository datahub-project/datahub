import Ember from 'ember';

export default Ember.LinkComponent.extend({
  tagName: 'li',

  attributeBindings: ['data-toggle', 'data-target'],

  hrefForA: Ember.computed('models', 'qualifiedRouteName', function () {
    let qualifiedRouteName = this.get('qualifiedRouteName');
    let models = this.get('models');

    if (this.get('loading')) {
      return this.get('loadingHref');
    }

    let routing = this.get('_routing');
    let queryParams = this.get('queryParams.values');
    return routing.generateURL(qualifiedRouteName, models, queryParams);
  })
});
