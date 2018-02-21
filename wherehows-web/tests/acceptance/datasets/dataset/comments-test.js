import { skip } from 'qunit';
import moduleForAcceptance from 'wherehows-web/tests/helpers/module-for-acceptance';
import { visit, find, currentURL, waitUntil } from 'ember-native-dom-helpers';
import defaultScenario from 'wherehows-web/mirage/scenarios/default';
import appLogin from 'wherehows-web/tests/helpers/login/test-login';

moduleForAcceptance('Acceptance | datasets/dataset/comments', {
  async beforeEach() {
    appLogin();
  }
});

// feature not yet supported in v2
skip('visiting /datasets/dataset/comments', async function(assert) {
  assert.expect(2);
  defaultScenario(server);
  const url = '/datasets/12345/comments';

  await visit(url);
  assert.equal(currentURL(), url, 'comments route is visitable');

  await waitUntil(() => find('.ivy-tabs-tab'));
  assert.equal(find('.ivy-tabs-tab.active').textContent.trim(), 'Comments', 'comments tab is selected');
});
