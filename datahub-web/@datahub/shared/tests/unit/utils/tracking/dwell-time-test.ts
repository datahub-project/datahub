import DwellTime from '@datahub/shared/utils/tracking/dwell-time';
import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import { triggerEvent } from '@ember/test-helpers';
import { searchRouteName } from '@datahub/shared/constants/tracking/site-search-tracking';

module('Unit | Utility | tracking/dwell-time', function(hooks): void {
  setupTest(hooks);

  test('instantiation succeeds', function(assert): void {
    const routerService = this.owner.lookup('service:router');
    const dwellTime = new DwellTime(searchRouteName, routerService);
    assert.ok(dwellTime);
  });

  test('dwellTime properties', function(assert): void {
    const routerService = this.owner.lookup('service:router');
    const dwellTime = new DwellTime(searchRouteName, routerService);

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

  test('dwellTime cleanup', function(assert): void {
    assert.expect(1);

    const dwellTime = new DwellTime(searchRouteName, this.owner.lookup('service:router'));

    dwellTime.unbindListeners = (): void => {
      assert.ok(true, 'expected onDestroy to be called when the beforeunload event is triggered');
    };

    triggerEvent(document.body, 'beforeunload');
  });
});
