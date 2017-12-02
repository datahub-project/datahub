import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { triggerEvent } from 'ember-native-dom-helpers';
import { missingPolicyText, purgePolicyProps } from 'wherehows-web/constants';

moduleForComponent('purge-policy', 'Integration | Component | purge policy', {
  integration: true
});

const policyList = '.purge-policy-list';

test('it renders', function(assert) {
  assert.expect(1);

  this.render(hbs`{{purge-policy}}`);

  assert.equal(document.querySelector(policyList).tagName, 'UL', 'expected component element is rendered');
});

test('it renders each purge policy in edit mode', function(assert) {
  assert.expect(1);
  const purgePoliciesLength = Object.keys(purgePolicyProps).length;

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
