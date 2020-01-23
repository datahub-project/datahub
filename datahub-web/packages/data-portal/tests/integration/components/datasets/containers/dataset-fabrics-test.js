import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, waitFor } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

const containerTestClass = 'container-testing-class';

module('Integration | Component | datasets/containers/dataset-fabrics', function(hooks) {
  setupRenderingTest(hooks);

  test('dataset fabric container rendering', async function(assert) {
    assert.expect(2);

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
  });

  test('dataset fabric container yielding', async function(assert) {
    assert.expect(1);

    const { server } = this;
    const { uri } = server.create('datasetView');
    this.set('urn', uri);
    this.set('class', containerTestClass);

    await render(hbs`
        {{#datasets/containers/dataset-fabrics class=class urn=urn as |container|}}
          <div class="yielded-content">{{container.urn}}</div>
        {{/datasets/containers/dataset-fabrics}}
      `);

    assert.equal(
      find(`.${containerTestClass} .yielded-content`).textContent.trim(),
      `${this.get('urn')}`,
      'it should yield the dataset urn contextually'
    );
  });
});
