import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll, triggerEvent, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import EmberObject from '@ember/object';

module('Integration | Component | radio-button-composer', function(hooks): void {
  setupRenderingTest(hooks);

  class Person extends EmberObject {
    ssn?: string;
    isEqual(other: Person): boolean {
      return this.ssn === other.ssn;
    }
  }

  const matchingSSN = '123-45-6789';
  const alice = Person.create({ name: 'Alice', ssn: matchingSSN });
  const alice2 = Person.create({ name: 'Alice 2', ssn: matchingSSN });
  const bob = Person.create({ name: 'Bob', ssn: '999-99-9999' });

  const disabledClass = 'paralyzed-pikachu';

  test('decorated properties behave as expected', async function(assert): Promise<void> {
    this.setProperties({
      disabledClass,
      value: 'electrify',
      mouseEnter() {
        assert.ok(true, 'Function got called on mouseEnter');
      }
    });
    await render(hbs`<RadioButtonComposer
                       @disabledClass={{this.disabledClass}}
                       @name="testA"
                       @groupValue="groupA"
                       @value={{this.value}}
                       @onMouseEnter={{this.mouseEnter}}
                    >
                      {{value}}
                    </RadioButtonComposer>`);

    // Ensures the function assert was called.
    assert.expect(5);
    assert.equal(this.element.textContent?.trim(), 'electrify', 'renders expected value');
    assert.equal(findAll(`.${disabledClass}`).length, 0, 'Does not disable when not supposed to');

    await triggerEvent('span', 'mouseenter');

    await render(hbs`<RadioButtonComposer
                       @disabledClass={{this.disabledClass}}
                       @name="testB"
                       @groupValue="groupB"
                       @value={{this.value}}
                       @onMouseEnter={{this.mouseEnter}}
                       @disabled={{true}} />`);

    assert.equal(findAll(`.${disabledClass}`).length, 1, 'Renders the disabled class');
    await triggerEvent('span', 'mouseenter');
  });

  test('begins checked when groupValue matches value', async function(assert) {
    assert.expect(1);

    await render(hbs`
      <RadioButtonComposer
        @groupValue='chosen-value'
        @value='chosen-value'
      />
    `);

    assert.equal(this.element.querySelector('input')?.checked, true);
  });

  test('either @class or class work fine', async function(assert) {
    assert.expect(2);

    await render(hbs`
      <RadioButtonComposer
        @groupValue='chosen-value'
        @value='chosen-value'
        @class='myclass'
      />
    `);

    assert.dom('.myclass').exists();

    await render(hbs`
      <RadioButtonComposer
        @groupValue='chosen-value'
        @value='chosen-value'
        class='myclass'
      />
    `);

    assert.dom('.myclass').exists();
  });

  test('it updates when clicked, and triggers the `changed` action', async function(assert) {
    assert.expect(5);

    let changedActionCallCount = 0;
    this.set('changed', (value: string) => {
      changedActionCallCount++;
      this.set('groupValue', value);
    });

    this.set('groupValue', 'initial-group-value');

    await render(hbs`
      <RadioButtonComposer
          @groupValue={{this.groupValue}}
          @value='component-value'
          @onChange={{fn this.changed}}
      />
    `);

    assert.equal(changedActionCallCount, 0);
    assert.equal(this.element.querySelector('input')?.checked, false);

    await click('input');

    assert.equal(this.element.querySelector('input')?.checked, true, 'updates element property');
    assert.equal(this.get('groupValue'), 'component-value', 'updates groupValue');

    assert.equal(changedActionCallCount, 1);
  });

  test('when no action is passed, updating does not error', async function(assert) {
    assert.expect(2);

    this.set('groupValue', 'initial-group-value');

    await render(hbs`
      <RadioButtonComposer
        @groupValue={{this.groupValue}}
        @value='component-value'
      />
    `);

    assert.equal(this.element.querySelector('input')?.checked, false, 'starts unchecked');

    await click('input');

    assert.equal(this.element.querySelector('input')?.checked, true, 'updates element property');
  });

  test('it updates when the browser change event is fired', async function(assert) {
    let changedActionCallCount = 0;
    this.set('changed', (value: string) => {
      changedActionCallCount++;
      this.set('groupValue', value);
    });

    this.set('groupValue', 'initial-group-value');

    await render(hbs`
      <RadioButtonComposer
          @groupValue={{this.groupValue}}
          @value='component-value'
          @onChange={{fn this.changed}}
      />
    `);

    assert.equal(changedActionCallCount, 0);
    assert.equal(this.element.querySelector('input')?.checked, false);

    await triggerEvent('input', 'change');

    assert.equal(this.element.querySelector('input')?.checked, true, 'updates DOM property');
    assert.equal(this.get('groupValue'), 'component-value', 'updates groupValue');
    assert.equal(changedActionCallCount, 1);
  });

  test('it gives the label of a wrapped checkbox a `checked` className', async function(assert) {
    assert.expect(4);

    this.set('groupValue', 'initial-group-value');
    this.set('value', 'component-value');

    await render(hbs`
      <RadioButtonComposer
        @groupValue={{this.groupValue}}
        @value={{this.value}}
        class='blue-radio'
      >
        Blue
      </RadioButtonComposer>
    `);

    assert.dom('label').doesNotHaveClass('checked');

    this.set('value', 'initial-group-value');

    assert.dom('label').hasClass('checked', 'has class `checked`');
    assert.dom('label').hasClass('ember-radio-button', 'has class `ember-radio-button`');
    assert.dom('label').hasClass('blue-radio', 'has class `blue-radio`');
  });

  test('providing `checkedClass` gives the label a custom classname when the radio is checked', async function(assert) {
    assert.expect(5);

    this.set('groupValue', 'initial-group-value');
    this.set('value', 'component-value');

    await render(hbs`
      <RadioButtonComposer
        @groupValue={{this.groupValue}}
        @value={{this.value}}
        @checkedClass="my-custom-class"
        class='blue-radio'
      >
        Blue
      </RadioButtonComposer>
    `);

    assert.dom('label').doesNotHaveClass('my-custom-class', 'does not have user-provided checkedClass');

    this.set('value', 'initial-group-value');

    assert.dom('label').doesNotHaveClass('checked', 'does not have the `checked` class');
    assert.dom('label').hasClass('my-custom-class', 'has user-provided checkedClass');
    assert.dom('label').hasClass('ember-radio-button', 'has class `ember-radio-button`');
    assert.dom('label').hasClass('blue-radio', 'has class `blue-radio`');
  });

  test('it updates when setting `value`', async function(assert) {
    assert.expect(3);

    this.set('groupValue', 'initial-group-value');
    this.set('value', 'component-value');

    await render(hbs`
      <RadioButtonComposer
        @groupValue={{this.groupValue}}
        @value={{this.value}}
      />
    `);

    assert.equal(this.element.querySelector('input')?.checked, false);

    this.set('value', 'initial-group-value');

    assert.equal(this.element.querySelector('input')?.checked, true);

    this.set('value', 'component-value');

    assert.equal(this.element.querySelector('input')?.checked, false);
  });

  test('begins disabled when disabled is true', async function(assert) {
    assert.expect(1);

    await render(hbs`<RadioButtonComposer
      @disabled={{true}}
    />`);

    assert.dom('input').isDisabled();
  });

  test('updates disabled when the disabled attribute changes', async function(assert) {
    this.set('isDisabled', false);

    await render(hbs`<RadioButtonComposer
      @disabled={{this.isDisabled}}
    />`);

    assert.dom('input').isNotDisabled();

    this.set('isDisabled', true);

    assert.dom('input').isDisabled();

    this.set('isDisabled', false);

    assert.dom('input').isNotDisabled();
  });

  test('begins with the `required` and `name` attributes when specified', async function(assert) {
    await render(hbs`<RadioButtonComposer
      required={{true}}
      name='colors'
    />`);

    assert.dom('input').hasAttribute('required');
    assert.dom('input').hasAttribute('name', 'colors');
  });

  test('updates the `required` attribute when the property changes', async function(assert) {
    this.set('isRequired', false);
    await render(hbs`<RadioButtonComposer
      required={{this.isRequired}}
    />`);

    assert.dom('input').isNotRequired();

    this.set('isRequired', true);

    assert.dom('input').isRequired();

    this.set('isRequired', false);

    assert.dom('input').isNotRequired();
  });

  test('updates the `name` attribute when the property changes', async function(assert) {
    this.set('name', undefined);
    await render(hbs`<RadioButtonComposer
      name={{this.name}}
    />`);

    assert.dom('input').doesNotHaveAttribute('name');

    this.set('name', 'colors');

    assert.dom('input').hasAttribute('name', 'colors');
  });

  test('uses a layout, tagName=label, when given a template', async function(assert) {
    await render(hbs`<RadioButtonComposer>Red</RadioButtonComposer>`);
    assert.dom('label').containsText('Red');
    assert.dom('input[type=radio]').exists();
  });

  test('it checks the input when the label is clicked and has a `for` attribute', async function(assert) {
    assert.expect(2);

    this.set('value', 'component-value');

    await render(hbs`
      <RadioButtonComposer
        @radioId='green-0'
        @radioClass='my-radio-class'
        @groupValue='initial-group-value'
        @value={{value}}
      >
        Green
      </RadioButtonComposer>
    `);

    await click('label');

    assert.dom('label').hasAttribute('for', 'green-0');
    assert.dom('input').hasAttribute('id', 'green-0');
  });

  test('it updates when setting `value` with isEqual', async function(assert) {
    assert.expect(3);

    this.set('groupValue', alice);
    this.set('value', bob);

    await render(hbs`
      <RadioButtonComposer
        @groupValue={{this.groupValue}}
        @value={{this.value}}
      />
    `);

    assert.equal(this.element.querySelector('input')?.checked, false);

    this.set('value', alice2);

    assert.equal(this.element.querySelector('input')?.checked, true);

    this.set('value', bob);

    assert.equal(this.element.querySelector('input')?.checked, false);
  });

  test('it binds `aria-labelledby` when specified', async function(assert) {
    assert.expect(2);

    this.set('ariaLabelledby', 'green-label');

    await render(hbs`
      <RadioButtonComposer
        @groupValue='initial-group-value'
        @value='value'
        aria-labelledby={{this.ariaLabelledby}}
      >
        Green
      </RadioButtonComposer>
    `);

    assert.dom('input').hasAttribute('aria-labelledby', 'green-label');
    assert.dom('input[value="value"]').hasAttribute('aria-labelledby', 'green-label');
  });

  test('it binds `aria-describedby` when specified', async function(assert) {
    assert.expect(1);

    this.set('ariaDescribedby', 'green-label');

    await render(hbs`
    <RadioButtonComposer
      @groupValue='initial-group-value'
      @value='value'
      aria-describedby={{this.ariaDescribedby}}
    >
      Green
    </RadioButtonComposer>
  `);
    assert.dom('input').hasAttribute('aria-describedby', 'green-label');
  });
});
