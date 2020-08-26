import { visit, click } from '@ember/test-helpers';
import { TestContext } from 'ember-test-helpers';
import { getQueue } from '@datahub/shared/test-helpers/tracking';
import { IBaseTrackingEvent } from '@datahub/shared/types/tracking/event-tracking';
import UnifiedTracking from '@datahub/shared/services/unified-tracking';

/**
 * Asynchronously navigates to a url and clicks on a search result item
 * @param {string} url
 * @returns {Promise<void>}
 */
export const navigateToSearchAndClickResult = async (url: string): Promise<void> => {
  await visit(url);
  await click('[data-content-name="searchResult@1"]');
};

/**
 * Stubs the tracking services trackEvent method to add received events to the tracking queue
 * @param {TestContext} testContext the test cases `this` reference
 * @param {ReturnType<typeof getQueue>} queue tracking activity queue
 * @returns {ReturnType<typeof getQueue>}
 */
export const mockTrackingEventQueue = (
  testContext: TestContext,
  queue: ReturnType<typeof getQueue>
): ReturnType<typeof getQueue> => {
  const trackingService: UnifiedTracking = testContext.owner.lookup('service:unified-tracking');

  // Stub tracking service with trackEvent stub function that adds seen events to supplied queue
  trackingService.trackEvent = ({ action, category, name = '' }: IBaseTrackingEvent): void => {
    // Push new events onto the queue. The the same queue needs to be mutated as expectation by the implementation
    queue.push([action, category, name]);
  };

  return queue;
};
