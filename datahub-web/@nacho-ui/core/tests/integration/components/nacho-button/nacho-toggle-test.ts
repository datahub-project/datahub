import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { baseToggleClass } from '@nacho-ui/core/components/nacho-button/nacho-toggle';

module('Integration | Component | nacho-toggle', function(hooks) {
  setupRenderingTest(hooks);

  const baseClass = `.${baseToggleClass}`;
  const rightButton = `${baseClass}__button--right`;
  const leftButton = `${baseClass}__button--left`;
  const activeButton = `${baseClass}__button--active`;

  test('it renders and behaves as expected', async function(assert) {
    await render(hbs`<NachoButton::NachoToggle/>`);
    assert.expect(7);
    assert.ok(this.element, 'Initial render is without renders');

    this.setProperties({
      value: 'Pikachu',
      leftValue: 'Pikachu',
      rightValue: 'Eevee'
    });

    this.set('onChangeValue', (newValue: string): void => {
      assert.equal(newValue, 'Eevee', 'Change value behaves as expected');
      this.set('value', newValue);
    });

    await render(hbs`<NachoButton::NachoToggle
                       @value={{this.value}}
                       @leftOptionValue={{this.leftValue}}
                       @leftOptionText={{this.leftValue}}
                       @rightOptionValue={{this.rightValue}}
                       @rightOptionText={{this.rightValue}}
                       @onChange={{this.onChangeValue}}
                     />`);

    assert.equal(findAll(baseClass).length, 1, 'Renders the main component');
    assert.equal(findAll('button').length, 2, 'Renders 2 "buttons" for toggling between two states');
    assert.equal(
      findAll(`${leftButton}${activeButton}`).length,
      1,
      'Based on parameters, left button should be currently active'
    );
    assert.equal(
      findAll(`${rightButton}${activeButton}`).length,
      0,
      'Based on parameters, right button should not be active'
    );

    await click(rightButton);

    assert.equal(
      findAll(`${rightButton}${activeButton}`).length,
      1,
      'After changing state, right button should be active'
    );
  });
});
