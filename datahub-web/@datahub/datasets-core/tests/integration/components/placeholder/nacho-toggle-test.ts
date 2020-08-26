import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { capitalize } from '@ember/string';

module('Integration | Component | placeholder/nacho-toggle', function(hooks): void {
  setupRenderingTest(hooks);

  const baseClass = '.nacho-toggle';
  const buttonClass = `${baseClass}__button`;
  const activeButton = `${buttonClass}--active`;
  const leftButton = `${buttonClass}--left`;
  const rightButton = `${buttonClass}--right`;

  test('it renders', async function(assert): Promise<void> {
    await render(hbs`{{placeholder/nacho-toggle}}`);
    assert.ok(this.element, 'Initial render is without errors');

    const leftOptionValue = 'pikachu';
    const rightOptionValue = 'eevee';

    this.setProperties({
      leftOptionValue,
      rightOptionValue,
      value: leftOptionValue,
      leftOptionText: capitalize(leftOptionValue),
      rightOptionText: capitalize(rightOptionValue),
      onChange: (newValue: string) => {
        this.set('value', newValue);
      }
    });

    await render(hbs`{{placeholder/nacho-toggle
                       value=value
                       leftOptionValue=leftOptionValue
                       leftOptionText=leftOptionText
                       rightOptionValue=rightOptionValue
                       rightOptionText=rightOptionText
                       onChange=(action onChange)
                     }}`);

    assert.equal(findAll(`${leftButton}${activeButton}`).length, 1, 'Left button should be the active on first');

    assert.equal(
      findAll(`${rightButton}${activeButton}`).length,
      0,
      'By that same logic, right button should not be active at first'
    );

    await click(rightButton);

    assert.equal(
      findAll(`${leftButton}${activeButton}`).length,
      0,
      'Left button should be inactive when we click the right side'
    );

    assert.equal(
      findAll(`${rightButton}${activeButton}`).length,
      1,
      'By that same logic, right button should now be active'
    );
  });
});
