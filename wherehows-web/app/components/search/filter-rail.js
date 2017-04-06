import Ember from 'ember';

const {
  Component
} = Ember;

export default Component.extend({
  tagName: 'ul',
  classNames: ['nacho-filter-rail'],

  checkboxComponent: 'search/checkbox-group',
  radioGroupComponent: 'search/radio-group',
  dropdownComponent: 'search/dropdown-selection',
  dateRangeComponent: 'search/daterange-selection'
});
