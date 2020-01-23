import Component from '@ember/component';

export default Component.extend({
  tagName: 'span',
  attributeBindings: ['draggable'],
  draggable: 'true',

  dragStart(e) {
    return e.dataTransfer.setData('text/data', this.get('content'));
  }
});
