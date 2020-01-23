/**
 * Merges global type defs for global modules on the window namespace.
 */
// eslint-disable-next-line @typescript-eslint/interface-name-prefix
interface Window {
  // global array for piwik tracking
  _paq: Array<Array<string>> & { push: (items: Array<string | boolean | number>) => number };
}
