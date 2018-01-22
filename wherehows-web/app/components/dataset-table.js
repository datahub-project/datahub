import Component from '@ember/component';
import { setProperties, computed, get, set } from '@ember/object';

// A default static list of page lengths
const defaultPageLengths = [10, 50, 100];

export default Component.extend({
  tagName: 'table',
  classNames: ['nacho-table nacho-table--bordered'],
  tableHeaderComponent: 'dataset-table-header',
  tableBodyComponent: 'dataset-table-body',
  tableRowComponent: 'dataset-table-row',
  tableFooterComponent: 'dataset-table-footer',
  sortDirection: 'asc',
  sortColumnWithName: null,
  page: 1,
  pageLengths: defaultPageLengths,

  /**
   * Initially sets the number of rows to be displayed in the table
   * to the first item found in the pageLengths, default or runtime.
   * Will reset to first element on subsequent updates or set value
   */
  limit: computed('pageLengths', function() {
    // Defaulting operation in the event the pageLengths is set to an falsey
    // value
    return get(this, 'pageLengths.firstObject') || defaultPageLengths.firstObject;
  }),

  /**
   * Check if the searchTerm occurs anywhere in the field name
   * TODO: Cache regex outside computed prop so it's not recalculated each time props change
   */
  data: computed('fields', 'searchTerm', function() {
    const searchTerm = get(this, 'searchTerm');
    const fieldRegex = new RegExp(`.*${searchTerm}.*`, 'i');

    return get(this, 'fields').filter(field => fieldRegex.test(String(field[get(this, 'filterBy')])));
  }),

  sortBy: computed('sortColumnWithName', 'sortDirection', function() {
    const { sortColumnWithName, sortDirection } = this.getProperties('sortColumnWithName', 'sortDirection');
    return `${sortColumnWithName}:${sortDirection}`;
  }).readOnly(),

  beginOffset: computed('page', 'limit', function() {
    return (get(this, 'page') - 1) * get(this, 'limit');
  }),

  endOffset: computed('beginOffset', function() {
    return get(this, 'beginOffset') + get(this, 'limit');
  }),

  actions: {
    sortDidChange(sortColumnWithName = null, sortDirection = 'asc') {
      if (!sortColumnWithName) {
        throw new Error(
          `Valid name not provided for table column sorting.
      Ensure that you have provided adequate arguments to sort change handler action.`
        );
      }

      return setProperties(this, {
        sortColumnWithName,
        sortDirection
      });
    },

    filterDidChange(searchTerm) {
      this.set('searchTerm', searchTerm);
      get(this, 'actions.pageDidChange').call(this);
    },

    pageDidChange(page = 1) {
      set(this, 'page', page);
    },

    limitDidChange(limit) {
      if (get(this, 'pageLengths').includes(limit)) {
        set(this, 'limit', limit);
      }
    }
  }
});
