import { Factory } from 'ember-cli-mirage';

export default Factory.extend({
  isInternal: true,
  tracking: () => ({
    trackers: {
      piwik: {
        piwikSiteId: 1337,
        piwikUrl: '//mock-tracking-url.pwn'
      }
    },
    isEnabled: true
  })
});
