import Ember from 'ember';

export default Ember.Component.extend({
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

  drop (e) {
    const data = e.dataTransfer.getData('text/data');
    this.sendAction('dropped', data, this.get('param'));
    this.set('dragClass', 'deactivated');
  }
});
