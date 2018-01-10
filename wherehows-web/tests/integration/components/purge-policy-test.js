import { moduleForComponent, test, skip } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { triggerEvent } from 'ember-native-dom-helpers';
import sinon from 'sinon';
import { missingPolicyText, purgePolicyProps, exemptPolicy, PurgePolicy } from 'wherehows-web/constants';
import { DatasetPlatform } from 'wherehows-web/constants/datasets/platform';

moduleForComponent('purge-policy', 'Integration | Component | purge policy', {
  integration: true,

  beforeEach() {
    this.xhr = sinon.useFakeXMLHttpRequest();
  },

  afterEach() {
    this.xhr.restore();
  }
});

const policyList = '.purge-policy-list';
const policyTypes = Object.keys(purgePolicyProps);

test('it renders', function(assert) {
  assert.expect(1);

  this.render(hbs`{{purge-policy}}`);

  assert.equal(document.querySelector(policyList).tagName, 'UL', 'expected component element is rendered');
});

test('it renders each purge policy in edit mode', function(assert) {
  assert.expect(1);
  const purgePoliciesLength = policyTypes.length;

  this.set('isEditable', true);
  this.render(hbs`{{purge-policy isEditable=isEditable}}`);

  assert.equal(
    document.querySelectorAll(`${policyList} [type=radio]`).length,
    purgePoliciesLength,
    'all policies are rendered'
  );
});

test('it triggers the onPolicyChange action', function(assert) {
  assert.expect(2);
  let onPolicyChangeActionCallCount = 0;

  this.set('isEditable', true);

  this.set('onPolicyChange', () => {
    assert.equal(++onPolicyChangeActionCallCount, 1, 'onPolicyChange action is invoked when change event is triggered');
  });

  this.render(hbs`{{purge-policy onPolicyChange=onPolicyChange isEditable=isEditable}}`);
  assert.equal(onPolicyChangeActionCallCount, 0, 'onPolicyChange action is not invoked on instantiation');

  triggerEvent(`${policyList} [type=radio]`, 'change');
});

test('it renders a user message if the purge policy is not set and is in readonly mode', function(assert) {
  assert.expect(1);

  this.render(hbs`{{purge-policy}}`);
  assert.equal(
    document.querySelector(`${policyList} p`).textContent,
    missingPolicyText,
    `${missingPolicyText} is rendered`
  );
});

skip('it indicates the currently selected purge policy', function(assert) {
  assert.expect(1);
  const selectedPolicy = PurgePolicy.ManualPurge;
  const platform = DatasetPlatform.MySql;

  this.set('isEditable', true);
  this.set('platform', platform);
  this.set('purgePolicy', selectedPolicy);

  this.render(hbs`{{purge-policy isEditable=isEditable purgePolicy=purgePolicy platform=platform}}`);

  assert.ok(
    document.querySelector(`${policyList} [type=radio][value=${selectedPolicy}]`).checked,
    `${selectedPolicy} radio is checked`
  );
});

skip('it focuses the comment element for exempt policy', function(assert) {
  assert.expect(1);

  const focusEditor = () => {
    assert.equal(++focusMethodCount, 1, 'focusEditor action is invoked');
  };
  let selectedPolicy = exemptPolicy;
  let platform = purgePolicyProps[selectedPolicy].platforms[0];
  let focusMethodCount = 0;

  this.setProperties({
    platform,
    focusEditor,
    isEditable: true,
    purgePolicy: selectedPolicy
  });

  this.render(
    hbs`{{purge-policy isEditable=isEditable purgePolicy=purgePolicy platform=platform focusEditor=focusEditor}}`
  );
});
