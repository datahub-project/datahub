import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { hdfsUrn, nonHdfsUrn } from 'wherehows-web/mirage/fixtures/urn';

moduleForComponent('datasets/upstream-dataset', 'Integration | Component | datasets/upstream dataset', {
  integration: true
});

test('it renders', function(assert) {
  this.render(hbs`{{datasets/upstream-dataset}}`);

  assert.ok(document.querySelector('.upstream-downstream-retention'), 'renders upstream-dataset as expected');
});

test('it renders upstream dataset properties', function(assert) {
  const upstreamLink = '.upstream-dataset__upstream-link';
  const upstreamsMetadata = [{}, {}];
  this.set('upstreamsMetadata', upstreamsMetadata);
  this.render(hbs`{{datasets/upstream-dataset upstreamsMetadata=upstreamsMetadata}}`);

  assert.equal(document.querySelectorAll(upstreamLink).length, 2, 'renders a link for each upstream dataset');
});
