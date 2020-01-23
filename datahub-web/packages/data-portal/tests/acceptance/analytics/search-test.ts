import { module, test } from 'qunit';
import { visit, currentURL } from '@ember/test-helpers';
import { setupApplicationTest } from 'ember-qunit';
import appLogin from 'wherehows-web/tests/helpers/login/test-login';
import searchResponse from 'wherehows-web/mirage/fixtures/search-response';
import { delay } from 'wherehows-web/utils/promise-delay';
import { getQueue, findInQueue } from 'wherehows-web/tests/helpers/analytics';
import {
  navigateToSearchAndClickResult,
  mockTrackingEventQueue
} from 'wherehows-web/tests/helpers/search/search-acceptance';
import { searchTrackingEvent } from '@datahub/tracking/constants/event-tracking/search';
import { getPiwikActivityQueue } from '@datahub/tracking/utils/piwik';

// Local test constants
const searchUrl = `/search?facets=()&keyword=${searchResponse.result.keywords}`;
const expectedSATClickEvent = searchTrackingEvent.SearchResultSATClick.action;
const successfulDwellTimeLength = 0.5;

module('Acceptance | analytics/search', function(hooks): void {
  setupApplicationTest(hooks);
  let paqBk: Window['_paq'];

  hooks.beforeEach(
    async (): Promise<void> => {
      // lets reset paq to avoid state from other tests (flacky tests)
      paqBk = window._paq;
      window._paq = [];
      await appLogin();
    }
  );

  hooks.afterEach((): void => {
    getPiwikActivityQueue(true);
    window._paq = paqBk;
  });

  test('analytics activity queue correctly tracks search result impressions', async function(assert): Promise<void> {
    const piwikContentImpressionActivityIdentifier = 'trackAllContentImpressions';
    const queue = getQueue();

    /**
     * Checks if the queue has the piwik content impression activity id
     * @param {Array<Array<string>>} queue
     * @returns {(Array<string> | undefined)}
     */
    const queueHasImpressionActivityId = (queue: Array<Array<string>>): Array<string> | undefined =>
      queue.find(findInQueue(piwikContentImpressionActivityIdentifier));

    assert.notOk(
      queueHasImpressionActivityId(queue),
      `expected piwik activity queue to not include ${piwikContentImpressionActivityIdentifier}`
    );

    await visit(searchUrl);

    assert.equal(currentURL(), searchUrl, `expected current url to be ${searchUrl}`);
    assert.ok(document.querySelector('[data-track-content]'), 'expected trackable content to be rendered');
    assert.ok(
      queueHasImpressionActivityId(queue),
      `expected piwik activity queue to include ${piwikContentImpressionActivityIdentifier}`
    );
  });

  test('transition from search page to entity page back to search page after successful dwell time is a SAT Click', async function(assert): Promise<
    void
  > {
    assert.expect(3);

    const queue = mockTrackingEventQueue(this, [...getQueue()]);
    // Potentially transition to useFakeTimers, added in separate rb
    this.owner.lookup('service:search').successfulDwellTimeLength = successfulDwellTimeLength;

    await navigateToSearchAndClickResult(searchUrl);

    assert.ok(
      currentURL().startsWith('/datasets'),
      'expected clicking a search result item to navigate to an entity page'
    );
    assert.notOk(
      queue.find(findInQueue(expectedSATClickEvent)),
      `expected the SAT Click event to not be in the activity queue`
    );

    await delay(successfulDwellTimeLength);
    await visit(searchUrl);

    assert.ok(
      queue.find(findInQueue(expectedSATClickEvent)),
      `expected the SAT clicked event ${expectedSATClickEvent} to be in the activity queue`
    );
  });

  test('transition from search page to entity page back to search page after unsuccessful dwell time is not a SAT Click', async function(assert): Promise<
    void
  > {
    assert.expect(2);

    const queue = mockTrackingEventQueue(this, [...getQueue()]);

    await navigateToSearchAndClickResult(searchUrl);

    assert.notOk(
      queue.find(findInQueue(expectedSATClickEvent)),
      `expected the SAT Click event to not be in the activity queue after search click`
    );

    await visit(searchUrl);

    assert.notOk(
      queue.find(findInQueue(expectedSATClickEvent)),
      `expected the SAT Click event to not be in the activity queue after return to search with no dwell time`
    );
  });

  test('transition from search page to entity page to non search page is a SAT Click', async function(assert): Promise<
    void
  > {
    assert.expect(3);
    const queue = mockTrackingEventQueue(this, [...getQueue()]);

    await navigateToSearchAndClickResult(searchUrl);

    assert.ok(
      currentURL().startsWith('/datasets'),
      'expected clicking a search result item to navigate to an entity page'
    );
    assert.notOk(
      queue.find(findInQueue(expectedSATClickEvent)),
      `expected the SAT Click event to not be in the activity queue`
    );

    try {
      await visit('/');
    } catch (error) {
      const { message } = error;
      if (message !== 'TransitionAborted') {
        throw error;
      }
    }

    assert.ok(
      queue.find(findInQueue(expectedSATClickEvent)),
      `expected the SAT clicked event ${expectedSATClickEvent} to be in the activity queue`
    );
  });
});
