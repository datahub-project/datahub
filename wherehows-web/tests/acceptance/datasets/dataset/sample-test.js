import { skip } from 'qunit';
import moduleForAcceptance from 'wherehows-web/tests/helpers/module-for-acceptance';
import { visit, find, currentURL, waitUntil } from 'ember-native-dom-helpers';
import defaultScenario from 'wherehows-web/mirage/scenarios/default';
import appLogin from 'wherehows-web/tests/helpers/login/test-login';

moduleForAcceptance('Acceptance | datasets/dataset/sample', {
  async beforeEach() {
    appLogin();
  }
});

skip('visiting /datasets/dataset/sample', async function(assert) {
  assert.expect(2);
  defaultScenario(server);
  const url = '/datasets/12345/sample';

  await visit(url);
  assert.equal(currentURL(), url, 'sample route is visitable');

  await waitUntil(() => find('.ivy-tabs-tab'));
  assert.equal(find('.ivy-tabs-tab.active').textContent.trim(), 'Sample Data', 'sample tab is selected');
});
