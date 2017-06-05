import Ember from 'ember';

export default Ember.Component.extend({
  classNames: ['nacho-select'],
  content: [],

  init() {
    this._super(...arguments);
    this.updateContent();
  },

  onSelectionChanged: Ember.observer('selected', 'values', function() {
    this.updateContent();
  }),

  /**
   * Parse and transform the values list into a list of objects with the currently
   * selected option flagged as `isSelected`
   */
  updateContent() {
    const selected = this.get('selected') || null;

    const options = this.get('values') || [];
    const content = options.map(option => {
      if (typeof option === 'object' && typeof option.value !== 'undefined') {
        const isSelected = option.value === selected;
        return { value: option.value, label: option.label, isSelected, isDisabled: option.isDisabled || false };
      }

      return { value: option, isSelected: option === selected };
    });

    this.set('content', content);
  },

  actions: {
    // Reflect UI changes in the component and bubble the `selectionDidChange` action
    change() {
      const { selectedIndex } = this.$('select')[0];
      const values = this.get('values');
      const _selected = values[selectedIndex];
      const selected = typeof _selected.value !== 'undefined' ? _selected.value : _selected;

      this.set('selected', selected);

      this.sendAction('selectionDidChange', _selected);
    }
  }
});
