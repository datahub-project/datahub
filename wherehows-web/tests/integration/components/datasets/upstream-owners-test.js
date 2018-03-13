import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { hdfsUrn, nonHdfsUrn } from 'wherehows-web/mirage/fixtures/urn';

moduleForComponent('datasets/upstream-owners', 'Integration | Component | datasets/upstream owners', {
  integration: true
});

test('it renders', function(assert) {
  this.render(hbs`{{datasets/upstream-owners}}`);

  assert.ok(document.querySelector('.upstream-owners-banner'), 'renders upstream-dataset as expected');
});

test('rendering nativeName', function(assert) {
  const titleElementQuery = '.upstream-owners-banner__title strong';
  const nativeName = 'Upstream Dataset';

  this.set('nativeName', nativeName);

  this.render(hbs`{{datasets/upstream-owners nativeName=nativeName}}`);

  assert.ok(
    document
      .querySelector(titleElementQuery)
      .textContent.trim()
      .includes(nativeName),
    'it renders upstream ownership properties'
  );
});

test('link to upstream dataset', function(assert) {
  this.set('upstreamUrn', hdfsUrn);

  this.render(hbs`{{datasets/upstream-owners upstreamUrn=upstreamUrn}}`);

  assert.ok(document.querySelector('.upstream-owners-banner a'), 'it creates a link to the upstream dataset');
});
