import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import FakeTimers from '@sinonjs/fake-timers';
import { renderingDone } from '@datahub/utils/test-helpers/rendering';
import { advanceAfterMs } from '@datahub/shared/components/health/last-updated';
import { run } from '@ember/runloop';

declare module '@ember/runloop' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface RunNamespace {
    cancelTimers(): void;
  }
}

module('Integration | Component | health/last-updated', function(hooks): void {
  setupRenderingTest(hooks);

  test('basic rendering and attribute updates', async function(assert): Promise<void> {
    const lastUpdatedSelector = '.health-score-last-update';
    const lastUpdated = Date.now();

    const clock = FakeTimers.install({
      now: lastUpdated
    });

    await render(hbs`<Health::LastUpdated @lastUpdated={{this.lastUpdated}} @small={{this.small}} />`);

    assert.dom(lastUpdatedSelector).doesNotExist('Expected the last updated element to not be rendered');
    assert.dom().hasText('', 'Expected have no text to be rendered when the last updated attribute is not provided');

    this.set('lastUpdated', lastUpdated);

    await renderingDone();

    assert.dom(lastUpdatedSelector).hasText('Last calculated a few seconds ago');

    clock.tick(advanceAfterMs);
    assert.dom(lastUpdatedSelector).hasText('Last calculated a minute ago');

    clock.tick(advanceAfterMs);
    assert.dom(lastUpdatedSelector).hasText('Last calculated 2 minutes ago');

    clock.tick(advanceAfterMs * 58);
    assert.dom(lastUpdatedSelector).hasText('Last calculated an hour ago');

    clock.uninstall();

    this.set('small', true);

    await renderingDone();

    assert.dom(lastUpdatedSelector).hasClass(/health-score-last-update--small/g);

    // http://ember-concurrency.com/docs/testing-debugging
    run.cancelTimers();
  });
});
