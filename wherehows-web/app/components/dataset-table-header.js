import Ember from 'ember';

const { Component } = Ember;

export default Component.extend({
  tagName: 'thead',
  tableColumnComponent: 'dataset-table-column'
});
