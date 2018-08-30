import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, waitUntil } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import sinon from 'sinon';

const platformsResponse = {
  platforms: [
    {
      name: 'platform1'
    },
    {
      name: 'platform2'
    }
  ]
};

module('Integration | Component | search/containers/search sources', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    this.sinonServer = sinon.createFakeServer();
    this.sinonServer.respondImmediately = true;
  });

  hooks.afterEach(function() {
    this.sinonServer.restore();
  });

  test('it renders', async function(assert) {
    this.sinonServer.respondWith('GET', /\/api\/v2\/list\/platforms/, [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify(platformsResponse)
    ]);

    await render(hbs`
      {{#search/containers/search-sources}}
        <div class="inner-content">Yielded Content</div>
      {{/search/containers/search-sources}}
    `);

    await waitUntil(() => find('.inner-content'));

    assert.equal(this.element.textContent.trim(), 'Yielded Content', 'inner content is rendered');
  });

  test('Platform sources yielded', async function(assert) {
    this.sinonServer.respondWith('GET', /\/api\/v2\/list\/platforms/, [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify(platformsResponse)
    ]);

    await render(hbs`
      {{#search/containers/search-sources as |container|}}
      <span class="inner-content">
      {{#each container.radioSources as |source|}}
      {{source.value}}
      {{/each}}
      </span>
      {{/search/containers/search-sources}}
    `);

    await waitUntil(() => find('.inner-content'));

    const text = document.querySelector('.inner-content').textContent.trim();
    assert.ok(
      ['all', 'platform1', 'platform2'].reduce((acc, curr) => acc && text.includes(curr), true),
      'it yields platform sources and `all`'
    );
  });
});
