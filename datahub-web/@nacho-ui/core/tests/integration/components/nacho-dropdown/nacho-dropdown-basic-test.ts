import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll, fillIn } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { TestContext } from 'ember-test-helpers';

const sampleOptions = [
  { label: 'Selected a value!', isDisabled: true, value: undefined },
  { label: 'Pikachu', value: 'pikachu' },
  { label: 'Eevee', value: 'eevee' },
  { label: 'Charmander', value: 'charmander' }
];

const componentClass = '.nacho-dropdown--basic';
const selectElement = `${componentClass} select`;
const optionElement = `${componentClass} option`;

module('Integration | Component | nacho-dropdown-basic', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function(this: TestContext) {
    this.setProperties({
      selected: undefined,
      options: sampleOptions
    });
  });

  test('it renders simple use case', async function(assert) {
    await render(hbs`<NachoDropdown::NachoDropdownBasic
                       @selected={{selected}}
                       @options={{options}}
                     />`);
    assert.ok(this.element, 'Initial render is without errors');
    assert.equal(findAll(componentClass).length, 1, 'Renders our dropdown component');
    assert.equal(findAll(selectElement).length, 1, 'Renders a select element');
    assert.equal(findAll(optionElement).length, 4, 'Renders 4 options');
    assert.equal(findAll(`${optionElement}:disabled`).length, 1, 'Renders one disabled option');
  });

  test('it renders block use case', async function(assert) {
    await render(hbs`<NachoDropdown::NachoDropdownBasic
                       @selected={{selected}}
                       @options={{options}}
                       as |dropdown|
                     >
                       <div class="test-block">
                         {{#with dropdown.item as |item|}}
                           {{item.label}}: {{item.value}}
                         {{/with}}
                       </div>
                    </NachoDropdown::NachoDropdownBasic>`);

    assert.ok(this.element, 'Still renders without errors');
    assert.equal(findAll(optionElement).length, 4, 'Renders 4 options still');
    assert.equal(findAll('.test-block').length, 4, 'Renders 4 test blocks as well');
  });

  test('it correctly uses handler to update changed selection', async function(assert) {
    const selectionTestValue = sampleOptions[2].value;

    this.set('selectionDidChange', (selection: unknown) => {
      assert.equal(selection, selectionTestValue, 'Selection is passed into handler function');
    });

    await render(hbs`<NachoDropdown::NachoDropdownBasic
                      @selected={{selected}}
                      @options={{options}}
                      @selectionDidChange={{selectionDidChange}}
                     />`);

    await fillIn(selectElement, 'eevee');
  });
});
