import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { buttonClass } from '@nacho-ui/button/components/nacho-sort-button';

module('Integration | Component | nacho-sort-button', function(hooks) {
  setupRenderingTest(hooks);

  const baseClass = `.${buttonClass}`;
  const rowClass = '.pokemon-detail-row';
  const sortDefaultIconClass = '.fa-sort';
  const sortUpIconClass = '.fa-sort-up';
  const sortDownIconClass = '.fa-sort-down';

  test('it renders and behaves as expected', async function(assert) {
    await render(hbs`{{nacho-sort-button}}`);
    assert.ok(this.element, 'Initial render is without errors');

    // We render the dummy component used to test the nacho button here as it already contains
    // a lot of useful testing logic that we shouldn't recreate here
    await render(hbs`{{test-sort-button}}`);

    assert.ok(this.element, 'Initial render is without errors');
    assert.equal(findAll(baseClass).length, 1, 'A nacho sort button was rendered');
    assert.equal(findAll(rowClass).length, 3, 'Renders 3 rows in our test table');

    // Testing sort icon button render
    assert.equal(findAll(sortDefaultIconClass).length, 1, 'Default sort rendered');
    assert.equal(findAll(sortDownIconClass).length, 0, 'Sanity check for sort icon');

    await click(baseClass);
    assert.equal(findAll(sortUpIconClass).length, 1, 'Rendering sort ascending');

    await click(baseClass);
    assert.equal(findAll(sortDownIconClass).length, 1, 'Rendering sort descending');
  });
});
