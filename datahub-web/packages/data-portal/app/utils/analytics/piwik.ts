import { scheduleOnce } from '@ember/runloop';

const _piwikActivityQueueFallbackQueue: Array<any> = [];
/**
 * Returns a reference to the globally available Piwik queue if available, otherwise an empty list is returned
 * @returns {Window['_paq']}
 */
export const getPiwikActivityQueue = (resetFallbackQueue?: boolean): Window['_paq'] => {
  const { _paq } = window;

  if (resetFallbackQueue) {
    _piwikActivityQueueFallbackQueue.length = 0;
  }

  return _paq || _piwikActivityQueueFallbackQueue;
};

/**
 * Tracks impressions for all rendered DOM content
 * This is scheduled in the afterRender queue to ensure that Piwik can accurately identify content blocks
 * that have been tagged with data-track-content data attributes
 * @returns {undefined}
 */
export const trackContentImpressions = (): undefined =>
  void scheduleOnce('afterRender', null, () => getPiwikActivityQueue().push(['trackAllContentImpressions']));
