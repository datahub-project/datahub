import Component from '@ember/component';
import { getProperties } from '@ember/object';

export default Component.extend({
  tagName: 'tr',

  classNames: ['dataset-author-record'],

  actions: {
    addOwner() {
      const { addOwner } = getProperties(this, 'addOwner');
      return addOwner();
    }
  }
});
