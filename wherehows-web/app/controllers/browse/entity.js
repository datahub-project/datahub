import Controller from '@ember/controller';

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
