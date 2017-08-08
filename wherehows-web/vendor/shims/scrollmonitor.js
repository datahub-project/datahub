(function() {
  function vendorModule() {
    'use strict';

    return { 'default': self['scrollMonitor'] };
  }

  define('scrollmonitor', [], vendorModule);
})();
