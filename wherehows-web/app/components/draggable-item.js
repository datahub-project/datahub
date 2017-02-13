import Ember from 'ember';

export default Ember.Component.extend({
  tagName: 'span',
  classNames: ['draggable-item'],
  attributeBindings: ['draggable'],
  draggable: 'true',

  dragStart(e) {
    return e.dataTransfer.setData('text/data', this.get('content'));
  }
});
