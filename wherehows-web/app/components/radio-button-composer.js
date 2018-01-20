import Component from '@ember/component';
import { get, getProperties } from '@ember/object';

export default Component.extend({
  tagName: '',

  didReceiveAttrs() {
    this._super(...arguments);

    ['name', 'groupValue', 'value'].forEach(attr => {
      if (!(attr in this)) {
        throw new Error(`Attribute '${attr}' is required to be passed in when instantiating this component.`);
      }
    });
  },

  actions: {
    changed() {
      const closureAction = get(this, 'attrs.changed');

      if (typeof closureAction === 'function') {
        return closureAction(...arguments);
      }
      get(this, 'changed')(...arguments);
    }
  }
});
