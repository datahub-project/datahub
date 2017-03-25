import Ember from 'ember';
import ApplicationRouteMixin
  from 'ember-simple-auth/mixins/application-route-mixin';

const {
  Route,
  run
} = Ember;

export default Route.extend(ApplicationRouteMixin, {
  init() {
    this._super(...arguments);
    run.scheduleOnce('afterRender', this, 'processLegacyDomOperations');
  },

  processLegacyDomOperations() {
    window.legacySearch();
    // TODO: DSS-6122 Refactor Remove tree legacy operations & references
    // window.legacyTree();
    window.legacyMain();
  }
});
