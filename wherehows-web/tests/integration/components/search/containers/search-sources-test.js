import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import sinon from 'sinon';
import { waitUntil, find } from 'ember-native-dom-helpers';

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
    this.server = sinon.createFakeServer();
    this.server.respondImmediately = true;
  });

  hooks.afterEach(function() {
    this.server.restore();
  });

  test('it renders', async function(assert) {
    this.server.respondWith('GET', /\/api\/v2\/list\/platforms/, [
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

    assert.equal(
      this.$()
        .text()
        .trim(),
      'Yielded Content',
      'inner content is rendered'
    );
  });

  test('Platform sources yielded', async function(assert) {
    this.server.respondWith('GET', /\/api\/v2\/list\/platforms/, [
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
