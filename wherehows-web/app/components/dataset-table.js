import Ember from 'ember';

const {
  Component,
  computed,
  setProperties,
  get,
  set
} = Ember;

export default Component.extend({
  tagName: 'table',
  tableHeaderComponent: 'dataset-table-header',
  tableBodyComponent: 'dataset-table-body',
  tableRowComponent: 'dataset-table-row',
  tableFooterComponent: 'dataset-table-footer',
  sortDirection: 'asc',
  sortColumnWithName: null,
  page: 1,
  limit: 5,
  pageLengths: [5, 10],

  data: computed('fields', 'searchTerm', function() {
    return get(this, 'fields').filter(field =>
      String(field[get(this, 'filterBy')])
        .toLowerCase()
        .includes(String(get(this, 'searchTerm').toLowerCase())));
  }),

  sortBy: computed('sortColumnWithName', 'sortDirection', function() {
    const { sortColumnWithName, sortDirection } = this.getProperties(
      'sortColumnWithName',
      'sortDirection'
    );
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
