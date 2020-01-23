import { module, test } from 'qunit';
import { visit, currentURL, find } from '@ember/test-helpers';
import { setupApplicationTest } from 'ember-qunit';
import appLogin from 'wherehows-web/tests/helpers/login/test-login';
import defaultScenario from 'wherehows-web/mirage/scenarios/default';
import { IMirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';

module('Acceptance | Entities I Own', function(hooks) {
  setupApplicationTest(hooks);

  test('Entities I own page works as expected', async function(this: IMirageTestContext, assert) {
    defaultScenario(this.server);
    await appLogin();
    await visit('/');
    await visit('/user/entity/datasets/own');

    assert.equal(currentURL(), '/user/entity/datasets/own', 'Should get to datasets I Own page');

    const title = this.element.querySelector('.search-results__title');

    assert.equal(title && title.textContent, 'Datasets I Own', 'Page title should be correct');

    const searchResults = this.element.querySelectorAll('.search-result');

    assert.equal(searchResults.length, 25, 'There should be 25 results');

    const browseAnchor = find('.user-ownership-subnav a');
    const browseAnchorLink = browseAnchor ? browseAnchor.getAttribute('href') : '';
    assert.ok(browseAnchorLink && browseAnchorLink.includes('/datasets'));

    assert.dom('.user-ownership-subnav').hasText('Browse all Datasets');
  });
});
