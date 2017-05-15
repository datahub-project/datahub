import Ember from 'ember';
const { Controller } = Ember;

/**
 * Handles query params for browse.entity route
 */
export default Controller.extend({
  queryParams: ['page', 'urn', 'size', 'name'],
  page: 1,
  urn: '',
  name: '',
  size: 10
});
