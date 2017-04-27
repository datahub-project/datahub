import Ember from 'ember';
const { Controller } = Ember;

/**
 * Handles query params for browse.entity route
 */
export default Controller.extend({
  queryParams: ['page', 'urn'],
  page: 1,
  urn: ''
});
