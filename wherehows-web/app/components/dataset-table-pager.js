import Ember from 'ember';

const {
  Component,
  computed,
  getProperties,
  get
} = Ember;

export default Component.extend({
  tagName: 'section',

  pages: computed('data', 'limit', function() {
    const { data, limit: rowsPerPage = 1 } = getProperties(
      this,
      'data',
      'limit'
    );
    let numberOfPages = 1;

    if (Array.isArray(data)) {
      const needsExtraPage = data.length % rowsPerPage;
      numberOfPages = Math.floor(data.length / rowsPerPage);
      needsExtraPage && ++numberOfPages;
    }

    return [...Array(numberOfPages).keys()].map(x => x + 1);
  }),

  actions: {
    onDecrementPage() {
      let page = get(this, 'page');

      if (page > 1) {
        get(this, 'onPageChanged')(page - 1);
      }
    },

    onIncrementPage() {
      let page = get(this, 'page');
      let max = Math.max(...get(this, 'pages'));

      if (page < max) {
        get(this, 'onPageChanged')(page + 1);
      }
    },

    changePage(page) {
      get(this, 'onPageChanged')(page);
    },

    changeLimit(limit) {
      get(this, 'onLimitChanged')(limit);
    }
  }
});
