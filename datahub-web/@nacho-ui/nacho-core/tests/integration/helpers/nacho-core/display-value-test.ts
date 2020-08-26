import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Helper | nacho-core/display-value', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders for basic cases', async function(assert) {
    const testCaseA = '1234';
    const testCaseB = 'Pikachu';
    const testCaseC = false;
    const testCaseD = ['hello', 'darkness', 'myold', 'friend'];

    this.setProperties({ testCaseA, testCaseB, testCaseC, testCaseD });

    await render(hbs`{{nacho-core/display-value testCaseA}}`);
    assert.equal(this.element.textContent?.trim(), testCaseA);

    await render(hbs`{{nacho-core/display-value testCaseB}}`);
    assert.equal(this.element.textContent?.trim(), testCaseB);

    await render(hbs`{{nacho-core/display-value testCaseC}}`);
    assert.equal(this.element.textContent?.trim(), 'No');

    await render(hbs`{{nacho-core/display-value testCaseD}}`);
    assert.equal(this.element.textContent?.trim(), 'hello, darkness, myold, friend');
  });
});
