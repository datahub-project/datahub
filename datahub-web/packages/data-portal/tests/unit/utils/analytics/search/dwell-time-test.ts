import DwellTime from 'wherehows-web/utils/analytics/search/dwell-time';
import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import { triggerEvent } from '@ember/test-helpers';
import { searchRouteName } from 'wherehows-web/constants/analytics/site-search-tracking';

module('Unit | Utility | analytics/search/dwell-time', function(hooks) {
  setupTest(hooks);

  test('instantiation succeeds', function(assert) {
    const routerService = this.owner.lookup('service:router');
    let dwellTime = new DwellTime(searchRouteName, routerService);
    assert.ok(dwellTime);
  });

  test('dwellTime properties', function(assert) {
    const routerService = this.owner.lookup('service:router');
    let dwellTime = new DwellTime(searchRouteName, routerService);

    assert.equal(dwellTime.startTime, 0, 'expected startTime to be 0 on instantiation');
    assert.equal(dwellTime.dwellTime, 0, 'expected dwellTime to be 0 on instantiation');
    assert.equal(
      dwellTime.searchRouteName,
      searchRouteName,
      `expected DwellTime#searchRouteName to match ${searchRouteName} on instantiation`
    );

    dwellTime.startTime = 10;
    dwellTime.resetDwellTimeTracking();
    assert.equal(dwellTime.startTime, 0, 'expected startTime to be reset by resetDwellTimeTracking');
  });

  test('dwellTime cleanup', function(assert) {
    assert.expect(1);

    const dwellTime = new DwellTime(searchRouteName, this.owner.lookup('service:router'));

    dwellTime.unbindListeners = () => {
      assert.ok(true, 'expected onDestroy to be called when the beforeunload event is triggered');
    };

    triggerEvent(document.body, 'beforeunload');
  });
});
