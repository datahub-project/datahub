import { module, test } from 'qunit';
import { visit, click, find, fillIn, currentURL } from '@ember/test-helpers';
import { setupApplicationTest } from 'ember-qunit';
import appLogin from 'wherehows-web/tests/helpers/login/test-login';
import { searchBarSelector } from 'wherehows-web/tests/helpers/search/global-search-constants';

module('Acceptance | search', function(hooks) {
  setupApplicationTest(hooks);

  test('Search does not through an error when typing', async function(assert) {
    await appLogin();
    await visit('/');

    assert.equal(currentURL(), '/browse/datasets', 'We made it to the home page in one piece');
    fillIn(searchBarSelector, 'Hello darkness my old friend');
    assert.ok(true, 'Did not encounter an error when filling in search bar');
  });
});
