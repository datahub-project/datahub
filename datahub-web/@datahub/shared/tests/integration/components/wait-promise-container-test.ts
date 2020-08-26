import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, settled } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { noop } from 'lodash';

module('Integration | Component | wait-promise-container', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    let testResolve: (value?: unknown) => void = noop;
    this.set(
      'promise',
      new Promise((resolve): void => {
        testResolve = resolve;
      })
    );
    await render(hbs`
      <WaitPromiseContainer @promise={{promise}} as |resolved|>
        {{resolved}}
      </WaitPromiseContainer>
    `);

    assert.equal(this.element.textContent && this.element.textContent.trim(), '');

    testResolve('test');

    // we need to wait to the rerender to happen as setting a property will trigger an ember loop
    await settled();
    assert.equal(this.element.textContent && this.element.textContent.trim(), 'test');
  });
});
