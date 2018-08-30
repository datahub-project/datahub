import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { nonHdfsUrn } from 'wherehows-web/mirage/fixtures/urn';

module('Integration | Component | datasets/containers/dataset-lineage-upstreams', function(hooks) {
  setupRenderingTest(hooks);

  test('component rendering', async function(assert) {
    assert.expect(2);

    this.set('urn', nonHdfsUrn);

    await render(hbs`
      {{#datasets/containers/dataset-lineage-upstreams urn=urn}}
        nested container content
      {{/datasets/containers/dataset-lineage-upstreams}}
    `);

    assert.ok(this.element, 'expect component to be rendered in DOM');
    assert.equal(
      this.element.textContent!.trim(),
      'nested container content',
      'expect container to render nested content'
    );
  });

  test('component yielding with a urn', async function(assert) {
    assert.expect(1);

    const { server }: any = this;
    const upstreamCount = 3;

    server.createList('datasetView', upstreamCount);

    this.set('urn', nonHdfsUrn);

    await render(hbs`
      {{#datasets/containers/dataset-lineage-upstreams urn=urn as |container|}}
        <ul class="container-list">
          {{#each container.upstreams}}
            <li></li>
          {{/each}}
        </ul>
      {{/datasets/containers/dataset-lineage-upstreams}}
    `);

    assert.equal(
      findAll('.container-list li')!.length,
      upstreamCount,
      'expect component to yield each upstream dataset'
    );
  });
});
