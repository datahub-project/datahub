import Ember from 'ember';

export default Ember.Component.extend({
  tagName: 'span',
  attributeBindings: ['draggable'],
  draggable: 'true',

  dragStart(e) {
    return e.dataTransfer.setData('text/data', this.get('content'));
  }
});
