import Ember from 'ember';
const {
  get,
  Component
} = Ember;

export default Component.extend({
  tagName: 'form',

  elementId: 'global-search-form',

  search: '',

  actions: {
    search() {
      this.sendAction('didSearch', get(this, 'search'));
    }
  }
});
