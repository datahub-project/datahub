import { visit, find, currentURL, waitUntil } from '@ember/test-helpers';
import { module, skip } from 'qunit';
import { setupApplicationTest } from 'ember-qunit';
import defaultScenario from 'wherehows-web/mirage/scenarios/default';
import appLogin from 'wherehows-web/tests/helpers/login/test-login';

module('Acceptance | datasets/dataset/schema', function(hooks) {
  setupApplicationTest(hooks);

  hooks.beforeEach(function() {
    appLogin();
  });

  skip('visiting /datasets/dataset/schema', async function(assert) {
    assert.expect(2);
    defaultScenario(this.server);
    const url = '/datasets/12345/schema';

    await visit(url);
    assert.equal(currentURL(), url, 'schema route is visitable');

    await waitUntil(() => find('.ivy-tabs-tab'));
    assert.equal(find('.ivy-tabs-tab.active').textContent.trim(), 'Schema', 'schema tab is selected');
  });
});
