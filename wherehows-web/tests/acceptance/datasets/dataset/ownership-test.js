import { skip } from 'qunit';
import moduleForAcceptance from 'wherehows-web/tests/helpers/module-for-acceptance';
import defaultScenario from 'wherehows-web/mirage/scenarios/default';
import { visit, find, currentURL, waitUntil } from 'ember-native-dom-helpers';
import appLogin from 'wherehows-web/tests/helpers/login/test-login';

moduleForAcceptance('Acceptance | datasets/dataset/ownership', {
  async beforeEach() {
    appLogin();
  }
});

skip('visiting /datasets/dataset/ownership', async function(assert) {
  assert.expect(2);
  defaultScenario(server);
  const url = '/datasets/12345/ownership';

  await visit(url);
  assert.equal(currentURL(), url, 'ownership route is visitable');

  await waitUntil(() => find('.ivy-tabs-tab'));
  assert.equal(find('.ivy-tabs-tab.active').textContent.trim(), 'Ownership', 'ownership tab is selected');
});
