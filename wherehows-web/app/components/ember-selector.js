import Ember from 'ember';

export default Ember.Component.extend({
  class: 'form-control',
  content: [],

  init() {
    this._super(...arguments);
    this.updateContent();
  },

  onSelectionChanged: Ember.observer('selected', function () {
    this.updateContent();
  }),

  /**
   * Parse and transform the values list into a list of objects with the currently
   * selected option flagged as `isSelected`
   */
  updateContent() {
    let selected = this.get('selected') || '';
    selected && (selected = String(selected).toLowerCase());

    const options = this.get('values') || [];
    const content = options.map(option => {
      if (typeof option === 'object' && typeof option.value !== 'undefined') {
        const isSelected = String(option.value).toLowerCase() === selected;
        return {value: option.value, label: option.label, isSelected};
      }

      return {value: option, isSelected: String(option).toLowerCase() === selected};
    });

    this.set('content', content);
  },

  actions: {
    // Reflect UI changes in the component and bubble the `selectionDidChange` action
    change() {
      const {selectedIndex} = this.$('select')[0];
      const values = this.get('values');
      const _selected = values[selectedIndex];
      const selected = typeof _selected.value !== 'undefined' ? _selected.value : _selected;

      this.set('selected', selected);

      this.sendAction('selectionDidChange', _selected);
    }
  }
});
