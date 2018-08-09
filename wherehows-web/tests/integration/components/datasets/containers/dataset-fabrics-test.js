import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { startMirage } from 'wherehows-web/initializers/ember-cli-mirage';
import { find, waitFor } from 'ember-native-dom-helpers';
import fabrics from 'wherehows-web/mirage/fixtures/fabrics';

module('Integration | Component | datasets/containers/dataset-fabrics', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    this.server = startMirage();
  });

  hooks.afterEach(function() {
    this.server.shutdown();
  });

  test('dataset fabric container rendering', async function(assert) {
    assert.expect(5);

    const containerTestClass = 'container-testing-class';
    const loadingSelector = '.ellipsis-loader';

    this.set('class', containerTestClass);
    await render(hbs`
        {{#datasets/containers/dataset-fabrics class=class}}
          rendered
        {{/datasets/containers/dataset-fabrics}}
      `);

    await waitFor(`.${containerTestClass}`);

    assert.equal(
      find(`.${containerTestClass}`).textContent.trim(),
      'rendered',
      'it should yield contents when no urn is provided'
    );

    const { server } = this;
    const { uri } = server.create('datasetView');
    this.set('urn', uri);

    await render(hbs`
        {{#datasets/containers/dataset-fabrics urn=urn}}
        {{/datasets/containers/dataset-fabrics}}
      `);

    await waitFor(loadingSelector);

    assert.ok(find(loadingSelector), 'it should render a loading indicator pending data fetching');

    await render(hbs`
        {{#datasets/containers/dataset-fabrics class=class urn=urn}}
          <div class="yielded-content">rendered with urn supplied</div>
        {{/datasets/containers/dataset-fabrics}}
      `);

    await waitFor(`.${containerTestClass} .yielded-content`);

    assert.equal(
      find(`.${containerTestClass} .yielded-content`).textContent.trim(),
      'rendered with urn supplied',
      'it should yield contents when a valid urn is provided'
    );

    await render(hbs`
        {{#datasets/containers/dataset-fabrics class=class urn=urn as |container|}}
          <div class="yielded-content">{{container.urn}}</div>
        {{/datasets/containers/dataset-fabrics}}
      `);

    await waitFor(`.${containerTestClass} .yielded-content`);

    assert.equal(
      find(`.${containerTestClass} .yielded-content`).textContent.trim(),
      `${this.get('urn')}`,
      'it should yield the dataset urn contextually'
    );

    await render(hbs`
        {{#datasets/containers/dataset-fabrics class=class urn=urn as |container|}}
          <div class="yielded-content">{{container.fabrics}}</div>
        {{/datasets/containers/dataset-fabrics}}
      `);

    await waitFor(`.${containerTestClass} .yielded-content`);

    assert.equal(
      find(`.${containerTestClass} .yielded-content`).textContent.trim(),
      `${fabrics}`,
      'it should yield the list of dataset fabrics contextually'
    );
  });
});
