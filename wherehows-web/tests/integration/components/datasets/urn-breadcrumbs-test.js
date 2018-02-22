import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { hdfsUrn, nonHdfsUrn } from 'wherehows-web/mirage/fixtures/urn';
import { datasetUrnRegexLI } from 'wherehows-web/utils/validators/urn';

moduleForComponent('datasets/urn-breadcrumbs', 'Integration | Component | datasets/urn breadcrumbs', {
  integration: true
});

const home = ['datasets'];

test('it renders only the home breadcrumb without a valid urn', function(assert) {
  this.render(hbs`{{datasets/urn-breadcrumbs}}`);
  const homeCrumb = document.querySelector('.nacho-breadcrumbs__crumb');

  assert.equal(homeCrumb.textContent.trim(), 'datasets', 'shows the home breadcrumb');
  assert.equal(
    document.querySelectorAll('.nacho-breadcrumbs__crumb').length,
    1,
    'only one breadcrumb is rendered when a urn is not provided'
  );
});

test('it renders breadcrumbs with a valid hdfs urn', function(assert) {
  const [, platform, segments] = datasetUrnRegexLI.exec(hdfsUrn);
  const segmentParts = segments.split('/').slice(1);
  let crumbs;

  this.set('urn', hdfsUrn);
  this.render(hbs`{{datasets/urn-breadcrumbs urn=urn}}`);

  crumbs = document.querySelectorAll('.nacho-breadcrumbs__crumb');

  assert.equal(crumbs.length, home.length + [platform].length + segmentParts.length, '');

  [...home, platform, ...segmentParts].forEach((node, index) =>
    assert.equal(node, crumbs[index].textContent.trim(), `breadcrumb ${index} has expected text ${node}`)
  );
});

test('it renders breadcrumbs with a valid non hdfs urn', function(assert) {
  const [, platform, segments] = datasetUrnRegexLI.exec(nonHdfsUrn);
  const segmentParts = segments.split('.');
  let crumbs;

  this.set('urn', nonHdfsUrn);
  this.render(hbs`{{datasets/urn-breadcrumbs urn=urn}}`);

  crumbs = document.querySelectorAll('.nacho-breadcrumbs__crumb');

  assert.equal(crumbs.length, home.length + [platform].length + segmentParts.length, '');

  [...home, platform, ...segmentParts].forEach((node, index) =>
    assert.equal(node, crumbs[index].textContent.trim(), `breadcrumb ${index} has expected text ${node}`)
  );
});
