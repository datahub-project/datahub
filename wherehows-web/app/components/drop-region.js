import Component from '@ember/component';
import { get } from '@ember/object';

export default Component.extend({
  classNames: ['drop-region'],
  classNameBindings: ['dragClass'],
  dragClass: 'deactivated',

  dragLeave(e) {
    e.preventDefault();
    this.set('dragClass', 'deactivated');
  },

  dragOver(e) {
    e.preventDefault();
    this.set('dragClass', 'activated');
  },

  drop(e) {
    const data = e.dataTransfer.getData('text/data');
    get(this, 'dropped')(data, get(this, 'param'));
    this.set('dragClass', 'deactivated');
  }
});
