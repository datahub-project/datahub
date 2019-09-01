import { module, test } from 'qunit';
import { visit, currentURL, find, triggerKeyEvent, click, waitFor } from '@ember/test-helpers';
import { setupApplicationTest } from 'ember-qunit';
import appLogin from '../helpers/login/test-login';
import { Keyboard } from 'wherehows-web/constants/keyboard';
import defaultScenario from 'wherehows-web/mirage/scenarios/default';
import { IMirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';

const nachoSearchInput = '.nacho-global-search input';
const browseCardElement = '.browse-card-container';

module('Acceptance | hot keys', function(hooks) {
  setupApplicationTest(hooks);

  test('We can focus the search bar when using the / key', async function(this: IMirageTestContext, assert) {
    defaultScenario(this.server);

    await appLogin();
    await visit('/');

    assert.equal(currentURL(), '/browse', 'Visiting index route redirects to /browse sub-route');

    // Navigate to an entity sub-route to enable search target

    await waitFor(browseCardElement, { timeout: 10000 });

    let testEntityCardElement = find(browseCardElement) as HTMLElement;
    await click(testEntityCardElement);

    const searchInput = find(nachoSearchInput);

    // Find interaction element on route transition
    await waitFor(browseCardElement, { timeout: 10000 });
    testEntityCardElement = find(browseCardElement) as HTMLElement;
    await triggerKeyEvent(testEntityCardElement, 'keyup', Keyboard.Slash);

    assert.equal(document.activeElement, searchInput, 'Search input becomes in focus upon pressing /');
  });
});
