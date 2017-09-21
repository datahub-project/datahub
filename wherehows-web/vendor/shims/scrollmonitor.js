/**
 * Shims scrollMonitor as an AMD module that can be imported with ES6 modules
 */
(function() {
  function vendorModule() {
    'use strict';

    return { default: self['scrollMonitor'] };
  }

  define('scrollmonitor', [], vendorModule);
})();
