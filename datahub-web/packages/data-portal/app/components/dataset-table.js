import Component from '@ember/component';
import { setProperties, getProperties, computed, get, set } from '@ember/object';
import buildSaneRegExp from 'wherehows-web/utils/validators/regexp';

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
   * Builds the regular expression filter based on the search term if provided, otherwise null
   * @type {ComputedProperty<null|RegExp>}
   */
  fieldFilter: computed('searchTerm', function() {
    const searchTerm = get(this, 'searchTerm');
    if (searchTerm) {
      return buildSaneRegExp(searchTerm, 'i');
    }

    return null;
  }),

  /**
   * Check if the searchTerm occurs anywhere in the field name
   * @type {DatasetTable.fields}
   */
  data: computed('fields', 'fieldFilter', function() {
    const { fieldFilter, fields = [] } = getProperties(this, ['fieldFilter', 'fields']);

    return fieldFilter ? fields.filter(field => fieldFilter.test(String(field[get(this, 'filterBy')]))) : fields;
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
