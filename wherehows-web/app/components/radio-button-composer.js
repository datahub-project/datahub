import Ember from 'ember';

const {
  Component,
  get
} = Ember;

export default Component.extend({
  tagName: '',

  didReceiveAttrs() {
    this._super(...arguments);
    const attrs = this.attrs;

    ['name', 'groupValue', 'value'].forEach(attr => {
      if (!(attr in attrs)) {
        throw new Error(
          `Attribute '${attr}' is required to be passed in when instantiating this component.`
        );
      }
    });
  },

  actions: {
    changed() {
      const closureAction = get(this, 'attrs.changed');

      if (typeof closureAction === 'function') {
        return closureAction(...arguments);
      }
      this.sendAction('changed', ...arguments);
    }
  }
});
