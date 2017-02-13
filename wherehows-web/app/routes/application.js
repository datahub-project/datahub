import Ember from 'ember';
import ApplicationRouteMixin from 'ember-simple-auth/mixins/application-route-mixin';

export default Ember.Route.extend(ApplicationRouteMixin, {
  init() {
    this._super(...arguments);
    Ember.run.scheduleOnce('afterRender', this, 'processLegacyDomOperations');
  },

  processLegacyDomOperations() {
    window.legacySearch();
    // window.legacyTree();
    window.legacyMain();
  }
})