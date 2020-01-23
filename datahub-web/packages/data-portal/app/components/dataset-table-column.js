import Component from '@ember/component';
import { computed } from '@ember/object';

export default Component.extend({
  tagName: 'th',
  classNameBindings: ['isColumnActive'],
  columnName: '',
  sortColumnWithName: '',
  sortDirection: null,

  isColumnActive: computed('sortColumnWithName', 'columnName', function() {
    const { sortColumnWithName, columnName } = this.getProperties('sortColumnWithName', 'columnName');
    return sortColumnWithName && sortColumnWithName === columnName;
  }),

  click() {
    const { columnName, sortDirection, isColumnActive } = this.getProperties(
      'columnName',
      'sortDirection',
      'isColumnActive'
    );

    if (columnName) {
      let updatedSortDirection;
      // if this column is not active, default sort direction to ascending, else toggle sort direction
      if (!isColumnActive) {
        updatedSortDirection = 'asc';
      } else {
        updatedSortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
      }

      this.get('sortDidChange')(columnName, updatedSortDirection);
      this.set('sortDirection', updatedSortDirection);
    }
  }
});
