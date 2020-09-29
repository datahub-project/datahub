import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Helper | list-includes', function(hooks): void {
  setupRenderingTest(hooks);

  test('it works for basic use cases', async function(assert): Promise<void> {
    const testList = ['pikachu', 'squirtle', 7];
    this.setProperties({
      testList,
      testValueA: 'pikachu',
      testValueB: 7,
      testValueC: 'ash ketchum'
    });

    await render(hbs`{{if (nacho-core/list-includes testList testValueA) "Pass" "Fail"}}`);
    assert.equal(
      this.element.textContent?.trim(),
      'Pass',
      'Testing string "pikachu", list-includes should return true'
    );

    await render(hbs`{{if (nacho-core/list-includes testList testValueB) "Pass" "Fail"}}`);
    assert.equal(this.element.textContent?.trim(), 'Pass', 'Testing number 7, list-includes should return true');

    await render(hbs`{{if (nacho-core/list-includes testList testValueC) "Pass" "Fail"}}`);
    assert.equal(
      this.element.textContent?.trim(),
      'Fail',
      'Testing string "ash ketchum", list-includes should return false'
    );
  });
});
