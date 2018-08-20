import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

import { startMirage } from 'wherehows-web/initializers/ember-cli-mirage';
import { nonHdfsUrn } from 'wherehows-web/mirage/fixtures/urn';

module('Integration | Component | datasets/containers/data-systems-count', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function(this: any) {
    this.server = startMirage();
  });

  hooks.afterEach(function(this: any) {
    this.server.shutdown();
  });

  test('component rendering', async function(assert) {
    assert.expect(2);

    await render(hbs`
      {{#datasets/containers/data-systems-count}}
        nested container content
      {{/datasets/containers/data-systems-count}}
    `);

    assert.ok(this.element, 'expect component to be rendered in DOM');
    assert.equal(
      this.element.textContent!.trim(),
      'nested container content',
      'expect container to render nested content'
    );
  });

  test('component rendering with a urn', async function(assert) {
    assert.expect(2);

    const { server }: any = this;
    server.create('datasetsCount');

    this.set('urn', nonHdfsUrn);

    await render(hbs`
      {{#datasets/containers/data-systems-count urn=urn as |container|}}
        {{container.count}}
      {{/datasets/containers/data-systems-count}}
    `);

    assert.ok(this.element, 'expect component to be rendered in DOM');

    const yieldedCount = parseInt(this.element.textContent!);
    assert.ok(yieldedCount >= 0, 'expect yielded value to be a number greater or equal to 0');
  });
});
