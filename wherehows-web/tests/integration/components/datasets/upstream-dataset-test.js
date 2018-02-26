import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { hdfsUrn, nonHdfsUrn } from 'wherehows-web/mirage/fixtures/urn';

moduleForComponent('datasets/upstream-dataset', 'Integration | Component | datasets/upstream dataset', {
  integration: true
});

test('it renders', function(assert) {
  this.render(hbs`{{datasets/upstream-dataset}}`);

  assert.ok(document.querySelector('.upstream-dataset-banner'), 'renders upstream-dataset as expected');
});

test('it renders upstream dataset properties', function(assert) {
  const titleElementQuery = '.upstream-dataset-banner__title strong';
  const descriptionElementQuery = '.upstream-dataset-banner__description strong';
  const nativeName = 'Upstream Dataset';
  const description = 'Upstream Dataset Description';

  this.set('nativeName', nativeName);
  this.set('description', description);

  this.render(hbs`{{datasets/upstream-dataset nativeName=nativeName description=description}}`);

  assert.ok(
    document
      .querySelector(titleElementQuery)
      .textContent.trim()
      .includes(nativeName),
    'renders nativeName'
  );
  assert.ok(
    document
      .querySelector(descriptionElementQuery)
      .textContent.trim()
      .includes(description),
    'renders description'
  );
});

test('it creates a link to the upstream dataset', function(assert) {
  this.set('upstreamUrn', hdfsUrn);

  this.render(hbs`{{datasets/upstream-dataset upstreamUrn=upstreamUrn}}`);

  assert.ok(document.querySelector('.upstream-dataset-banner a'), 'anchor element is rendered');
});
