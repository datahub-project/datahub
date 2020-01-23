import { module, test } from 'qunit';
import { visit } from '@ember/test-helpers';
import { setupApplicationTest } from 'ember-qunit';
import appLogin from 'wherehows-web/tests/helpers/login/test-login';

module('Acceptance | tracking', function(hooks) {
  /**
   * Local piwik event queue
   */
  let paq: Window['_paq'];

  /**
   * Real piwik event queue stored to be restored at a later point
   */
  let realPaq: Window['_paq'];

  /**
   * It will fetch from paq the event based on name
   * @param name name of the event
   */
  const getTrackingEvent = (name: string) => paq.find(([eventName]) => eventName === name) || [];

  setupApplicationTest(hooks);

  /**
   * Will replace paq events queue with a local one to capture events happening
   * only in the test, we want to reduce noise as much as possible
   */
  hooks.beforeEach(() => {
    paq = [];
    realPaq = window && window._paq ? window._paq : [];
    window._paq = paq;
  });

  /**
   * Restore paq event queue after the test
   */
  hooks.afterEach(() => {
    window._paq = realPaq;
  });

  test('Search tracks correctly', async function(assert) {
    const searchTerm = 'somerandomsearch';
    const SEARCH_EVENT_NAME = 'trackSiteSearch';

    await appLogin();
    await visit(`/search?keyword=${searchTerm}`);

    const [, keyword] = getTrackingEvent(SEARCH_EVENT_NAME);

    assert.equal(keyword, searchTerm, 'Search should be tracked correctly');
  });
});
