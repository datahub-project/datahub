import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { triggerEvent } from 'ember-native-dom-helpers';
import sinon from 'sinon';

import { purgePolicyProps, exemptPolicy, PurgePolicy, getSupportedPurgePolicies } from 'wherehows-web/constants';
import { DatasetPlatform } from 'wherehows-web/constants/datasets/platform';
import platforms from 'wherehows-web/mirage/fixtures/list-platforms';
import { ApiStatus } from 'wherehows-web/utils/api';

module('Integration | Component | purge policy', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    this.server = sinon.createFakeServer();
  });

  hooks.afterEach(function() {
    this.server.restore();
  });

  const policyList = '.purge-policy-list';
  const policyTypes = Object.keys(purgePolicyProps);
  const missingPolicyText = 'No policy';

  test('it renders', async function(assert) {
    assert.expect(1);

    await render(hbs`{{purge-policy}}`);

    assert.equal(document.querySelector(policyList).tagName, 'UL', 'expected component element is rendered');
  });

  test('it renders each purge policy in edit mode', async function(assert) {
    assert.expect(1);
    const purgePoliciesLength = policyTypes.length;

    this.set('isEditable', true);
    await render(hbs`{{purge-policy isEditable=isEditable}}`);

    assert.equal(
      document.querySelectorAll(`${policyList} [type=radio]`).length,
      purgePoliciesLength,
      'all policies are rendered'
    );
  });

  test('it triggers the onPolicyChange action', async function(assert) {
    assert.expect(2);
    let onPolicyChangeActionCallCount = 0;

    this.set('isEditable', true);

    this.set('onPolicyChange', () => {
      assert.equal(
        ++onPolicyChangeActionCallCount,
        1,
        'onPolicyChange action is invoked when change event is triggered'
      );
    });

    await render(hbs`{{purge-policy onPolicyChange=onPolicyChange isEditable=isEditable}}`);
    assert.equal(onPolicyChangeActionCallCount, 0, 'onPolicyChange action is not invoked on instantiation');

    triggerEvent(`${policyList} [type=radio]`, 'change');
  });

  test('it renders a user message if the purge policy is not set and is in readonly mode', async function(assert) {
    assert.expect(1);

    this.set('missingPolicyText', missingPolicyText);
    this.set('isEditable', false);
    await render(hbs`{{purge-policy missingPolicyText=missingPolicyText isEditable=isEditable}}`);
    assert.equal(
      document.querySelector(`${policyList}`).textContent.trim(),
      missingPolicyText,
      `${missingPolicyText} is rendered`
    );
  });

  test('it indicates the currently selected purge policy', async function(assert) {
    assert.expect(1);
    const selectedPolicy = PurgePolicy.ManualPurge;
    const platform = DatasetPlatform.MySql;

    this.set('isEditable', true);
    this.set('platform', platform);
    this.set('purgePolicy', selectedPolicy);
    this.set('supportedPurgePolicies', getSupportedPurgePolicies(platform, platforms));

    this.server.respondWith('GET', '/api/v1/list/platforms', [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify({ status: ApiStatus.OK, platforms })
    ]);

    await render(
      hbs`{{purge-policy isEditable=isEditable purgePolicy=purgePolicy platform=platform supportedPurgePolicies=supportedPurgePolicies}}`
    );
    this.server.respond();

    assert.ok(
      document.querySelector(`${policyList} [type=radio][value=${selectedPolicy}]`).checked,
      `${selectedPolicy} radio is checked`
    );
  });

  test('it focuses the comment element for exempt policy', async function(assert) {
    assert.expect(1);

    const focusEditor = () => {
      assert.equal(++focusMethodCount, 1, 'focusEditor action is invoked');
    };
    let selectedPolicy = exemptPolicy;
    let platform = DatasetPlatform.MySql;
    let focusMethodCount = 0;

    this.setProperties({
      platform,
      focusEditor,
      isEditable: true,
      purgePolicy: selectedPolicy
    });

    await render(
      hbs`{{purge-policy isEditable=isEditable purgePolicy=purgePolicy platform=platform focusEditor=focusEditor}}`
    );
  });
});
