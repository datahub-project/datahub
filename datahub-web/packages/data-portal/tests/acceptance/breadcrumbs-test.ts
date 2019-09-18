import { module, test } from 'qunit';
import { visit, currentURL, findAll, click, waitFor } from '@ember/test-helpers';
import { setupApplicationTest } from 'ember-qunit';
import appLogin from 'wherehows-web/tests/helpers/login/test-login';
import defaultScenario from 'wherehows-web/mirage/scenarios/default';
import { IMirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { getTextNoSpacesFromElements } from '@datahub/utils/test-helpers/dom-helpers';

module('Acceptance | breadcrumbs-smoke-test', function(hooks) {
  setupApplicationTest(hooks);

  test('Breadcrumbs Smoke Test', async function(this: IMirageTestContext, assert) {
    const categoryLinkClass = '.browse-category__link';
    const breadCrumbClass = '.nacho-breadcrumbs';
    const breadCrumbsClass = `${breadCrumbClass}__crumb`;
    defaultScenario(this.server);
    await appLogin();
    await visit('/');
    await visit('/browse/datasets?path=hdfs');
    await waitFor(categoryLinkClass);
    let categoryLinks = findAll(categoryLinkClass);
    let breadcrumbs = findAll(breadCrumbsClass);

    assert.equal(currentURL(), '/browse/datasets?path=hdfs');
    assert.equal(categoryLinks.length, 1, 'There is 1 folder in root hdfs');
    assert.equal(getTextNoSpacesFromElements(categoryLinks), 'some', 'Text match');
    assert.equal(breadcrumbs.length, 2, 'There are 2 links in breadcrumb');
    assert.equal(getTextNoSpacesFromElements(breadcrumbs), 'Datasetshdfs', 'Text match');

    // path /some
    await click(`${categoryLinkClass}:first-child`);
    await waitFor(categoryLinkClass);
    categoryLinks = findAll(categoryLinkClass);
    breadcrumbs = findAll(breadCrumbsClass);

    assert.equal(currentURL(), '/browse/datasets?path=hdfs%2Fsome');
    assert.equal(categoryLinks.length, 1, 'There is 1 folder in path');
    assert.equal(getTextNoSpacesFromElements(categoryLinks), 'path', 'Text match');
    assert.equal(breadcrumbs.length, 3, 'There are 3 links in breadcrumb');
    assert.equal(getTextNoSpacesFromElements(breadcrumbs), 'Datasetshdfssome', 'Text match');

    // path /some/path
    await click(`${categoryLinkClass}:first-child`);
    await waitFor(categoryLinkClass);
    categoryLinks = findAll(categoryLinkClass);
    breadcrumbs = findAll(breadCrumbsClass);

    assert.equal(currentURL(), '/browse/datasets?path=hdfs%2Fsome%2Fpath');
    assert.equal(categoryLinks.length, 1, 'There is 1 folder in path');
    assert.equal(getTextNoSpacesFromElements(categoryLinks), 'with', 'Text match');
    assert.equal(breadcrumbs.length, 4, 'There are 4 links in breadcrumb');
    assert.equal(getTextNoSpacesFromElements(breadcrumbs), 'Datasetshdfssomepath', 'Text match');

    // path /some/path/with
    await click(`${categoryLinkClass}:first-child`);
    await waitFor(categoryLinkClass, { count: 2 });
    categoryLinks = findAll(categoryLinkClass);
    breadcrumbs = findAll(breadCrumbsClass);

    assert.equal(currentURL(), '/browse/datasets?path=hdfs%2Fsome%2Fpath%2Fwith');
    assert.equal(categoryLinks.length, 2, 'There are 2 folder in path');
    assert.equal(getTextNoSpacesFromElements(categoryLinks), 'directoriesotherdir', 'Text match');
    assert.equal(breadcrumbs.length, 5, 'There are 5 links in breadcrumb');
    assert.equal(getTextNoSpacesFromElements(breadcrumbs), 'Datasetshdfssomepathwith', 'Text match');

    // path /some/path/with/directories
    await click(`${categoryLinkClass}:first-child`);
    await waitFor(categoryLinkClass, { count: 2 });
    categoryLinks = findAll(categoryLinkClass);
    breadcrumbs = findAll(breadCrumbsClass);

    assert.equal(currentURL(), '/browse/datasets?path=hdfs%2Fsome%2Fpath%2Fwith%2Fdirectories');
    assert.equal(categoryLinks.length, 2, 'There is only 2 datasets in path');
    assert.equal(getTextNoSpacesFromElements(categoryLinks), 'adataset1adataset2', 'Text match');
    assert.equal(breadcrumbs.length, 6, 'There are 6 links in breadcrumb');
    assert.equal(getTextNoSpacesFromElements(breadcrumbs), 'Datasetshdfssomepathwithdirectories', 'Text match');

    // path adataset1
    await click(`${categoryLinkClass}:first-child`);
    breadcrumbs = findAll(breadCrumbsClass);

    assert.equal(
      currentURL(),
      '/datasets/urn:li:dataset:(urn:li:dataPlatform:hdfs,%2Fsome%2Fpath%2Fwith%2Fdirectories%2Fadataset1,PROD)/schema'
    );
    assert.equal(breadcrumbs.length, 8, 'There are 8 links in breadcrumb');
    assert.dom(breadCrumbClass).hasText('Datasets PROD hdfs some path with directories adataset1');
  });
});
