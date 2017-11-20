import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { triggerEvent } from 'ember-native-dom-helpers';

import noop from 'wherehows-web/utils/noop';
import owners from 'wherehows-web/mirage/fixtures/owners';
import { OwnerType } from 'wherehows-web/utils/api/datasets/owners';

const [confirmedOwner, suggestedOwner] = owners;
const commonOwners = [];
const ownerTypes = Object.values(OwnerType);

moduleForComponent('dataset-author', 'Integration | Component | dataset author', {
  integration: true
});

test('it renders', function(assert) {
  this.set('removeOwner', noop);
  this.set('confirmSuggestedOwner', noop);
  this.set('owner', confirmedOwner);
  this.set('commonOwners', commonOwners);

  this.render(
    hbs`{{dataset-author confirmSuggestedOwner=confirmSuggestedOwner removeOwner=removeOwner owner=owner commonOwners=commonOwners}}`
  );

  assert.equal(document.querySelector('tr.dataset-author-record').tagName, 'TR');
});

test('triggers the removeOwner action when invoked', function(assert) {
  assert.expect(2);
  let removeActionCallCount = 0;

  this.set('removeOwner', () => {
    removeActionCallCount++;
    assert.equal(removeActionCallCount, 1, 'action is called once');
  });
  this.set('confirmSuggestedOwner', noop);
  this.set('owner', confirmedOwner);
  this.set('commonOwners', commonOwners);

  this.render(
    hbs`{{dataset-author confirmSuggestedOwner=confirmSuggestedOwner removeOwner=removeOwner owner=owner commonOwners=commonOwners}}`
  );

  assert.equal(removeActionCallCount, 0, 'action is not called on render');

  triggerEvent('.remove-dataset-author', 'click');
});

test('triggers the confirmSuggestedOwner action when invoked', function(assert) {
  assert.expect(2);
  let confirmSuggestedOwnerActionCallCount = 0;

  this.set('removeOwner', noop);
  this.set('confirmSuggestedOwner', () => {
    confirmSuggestedOwnerActionCallCount++;
    assert.equal(confirmSuggestedOwnerActionCallCount, 1, 'action is called once');
  });
  this.set('owner', suggestedOwner);
  this.set('commonOwners', commonOwners);

  this.render(
    hbs`{{dataset-author confirmSuggestedOwner=confirmSuggestedOwner removeOwner=removeOwner owner=owner commonOwners=commonOwners}}`
  );

  assert.equal(confirmSuggestedOwnerActionCallCount, 0, 'action is not called on render');

  triggerEvent('.confirm-suggested-dataset-author', 'click');
});

test('triggers the updateOwnerType action when invoked', function(assert) {
  assert.expect(2);

  this.set('removeOwner', noop);
  this.set('confirmSuggestedOwner', noop);
  this.set('updateOwnerType', (owner, type) => {
    assert.ok(confirmedOwner === owner, 'updateOwnerType action is invoked correct owner reference');
    assert.equal(type, confirmedOwner.type, 'updateOwnerType action is invoked with selected type');
  });
  this.set('owner', confirmedOwner);
  this.set('commonOwners', commonOwners);
  this.set('ownerTypes', ownerTypes);

  this.render(
    hbs`{{dataset-author confirmSuggestedOwner=confirmSuggestedOwner removeOwner=removeOwner owner=owner commonOwners=commonOwners updateOwnerType=updateOwnerType ownerTypes=ownerTypes}}`
  );

  triggerEvent('select', 'change');
});
