/**
 * Fallback array for Piwik activity queue "Window._paq"
 */
const _piwikActivityQueueFallbackQueue: Array<Array<string>> = [];

/**
 * Returns a reference to the globally available Piwik queue if available
 * If not a mutable list is returned as a fallback.
 * Ensure this is called after Piwik.js is loaded to receive a reference to the global paq
 * @returns {Window['_paq']}
 */
export const getPiwikActivityQueue = (resetFallbackQueue?: boolean): Window['_paq'] => {
  const { _paq } = window;

  if (resetFallbackQueue) {
    _piwikActivityQueueFallbackQueue.length = 0;
  }

  return _paq || _piwikActivityQueueFallbackQueue;
};
