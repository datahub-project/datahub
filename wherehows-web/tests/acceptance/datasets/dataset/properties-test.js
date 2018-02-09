import { test } from 'qunit';
import moduleForAcceptance from 'wherehows-web/tests/helpers/module-for-acceptance';
import { visit, click, find, fillIn, currentURL, waitUntil } from 'ember-native-dom-helpers';
import { authenticationUrl, testUser, testPassword } from 'wherehows-web/tests/helpers/login/constants';
import {
  loginUserInput,
  loginPasswordInput,
  loginSubmitButton
} from 'wherehows-web/tests/helpers/login/page-element-constants';
import defaultScenario from 'wherehows-web/mirage/scenarios/default';

moduleForAcceptance('Acceptance | datasets/dataset/properties', {
  async beforeEach() {
    await visit(authenticationUrl);
    await fillIn(loginUserInput, testUser);
    await fillIn(loginPasswordInput, testPassword);
    await click(loginSubmitButton);
  }
});

test('visiting /datasets/dataset/properties', async function(assert) {
  assert.expect(2);
  defaultScenario(server);
  const url = '/datasets/12345/properties';

  await visit(url);
  assert.equal(currentURL(), url, 'properties route is visitable');

  await waitUntil(() => find('.ivy-tabs-tab'));
  assert.equal(find('.ivy-tabs-tab.active').textContent.trim(), 'Properties', 'properties tab is selected');
});
