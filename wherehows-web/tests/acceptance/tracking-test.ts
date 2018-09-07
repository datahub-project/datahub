import { module, test } from 'qunit';
import { visit } from '@ember/test-helpers';
import { setupApplicationTest } from 'ember-qunit';
import appLogin from 'wherehows-web/tests/helpers/login/test-login';

module('Acceptance | tracking', function(hooks) {
  setupApplicationTest(hooks);

  test('Search tracks correctly', async function(assert) {
    const searchTerm = 'somerandomsearch';
    const paq = window && window._paq ? window._paq : [];
    const NAME = 0;
    const KEYWORD = 1;
    const SEARCH_EVENT_NAME = 'trackSiteSearch';

    // mocking paq qeue if not available
    window._paq = paq;

    await appLogin();
    await visit(`/search?keyword=${searchTerm}`);

    const searchEvent = paq.find(event => event[NAME] === SEARCH_EVENT_NAME);

    assert.equal(searchEvent[KEYWORD], searchTerm, 'Search should be tracked correctly');
  });
});
