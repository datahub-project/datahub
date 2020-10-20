import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { NachoDropdownOptions } from '@nacho-ui/core/types/nacho-dropdown';

module('Integration | Component | nacho-dropdown-power-select', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    assert.expect(7);
    const options: NachoDropdownOptions<string> = [
      { label: 'eletric pokemon', value: '', isCategoryHeader: true },
      { label: 'pikachu', value: 'pikachu' },
      { label: 'raichu', value: 'raichu' },
      { label: 'fire pokemon', value: '', isCategoryHeader: true },
      { label: 'charmander', value: 'charmander' }
    ];

    this.setProperties({
      options,
      onSelectionChange(selection: string): void {
        assert.equal(selection, 'pikachu', 'The correct option was passed back to our component');
      }
    });

    await render(hbs`
      <NachoDropdown::NachoDropdownPowerSelect
        @selected={{this.selected}}
        @selectionDidChange={{this.onSelectionChange}}
        @options={{this.options}}
        @placeholder="Please select a pokemon"
      />
    `);

    assert.ok(this.element, 'Initial render is without errors');
    assert.dom('.ember-power-select-placeholder').hasText('Please select a pokemon');
    assert.dom('.ember-basic-dropdown-content').doesNotExist('Untriggered dropdown list');
    assert.dom('.nacho-dropdown__category-header').doesNotExist('No content while dropdown not triggered');

    await click('.ember-basic-dropdown-trigger');

    assert.dom('.nacho-dropdown__category-header').exists();
    assert.equal(findAll('.nacho-dropdown__category-header').length, 2, 'Renders headers as expected');

    await click('.ember-power-select-option[data-option-index="1"]');
  });
});
