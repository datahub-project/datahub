import Component from '@ember/component';
import { get } from '@ember/object';

export default Component.extend({
  tagName: 'tr',

  classNames: ['dataset-author-record'],

  actions: {
    addOwner() {
      const { addOwner } = get(this, 'addOwner');
      return addOwner();
    }
  }
});
