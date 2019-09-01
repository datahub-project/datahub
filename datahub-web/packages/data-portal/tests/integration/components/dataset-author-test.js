import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, triggerEvent } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

import { noop } from 'wherehows-web/utils/helpers/functions';
import owners from 'wherehows-web/mirage/fixtures/owners';
import { OwnerType } from 'wherehows-web/utils/api/datasets/owners';

const [confirmedOwner, suggestedOwner] = owners;
const commonOwners = [];
const ownerTypes = Object.values(OwnerType);

module('Integration | Component | dataset author', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.set('removeOwner', noop);
    this.set('confirmSuggestedOwner', noop);
    this.set('author', { owner: confirmedOwner });
    this.set('commonOwners', commonOwners);

    await render(
      hbs`{{dataset-author confirmSuggestedOwner=confirmSuggestedOwner removeOwner=removeOwner owner=author commonOwners=commonOwners}}`
    );

    assert.equal(document.querySelector('tr.dataset-author-record').tagName, 'TR');
  });

  test('triggers the removeOwner action when invoked', async function(assert) {
    assert.expect(2);
    let removeActionCallCount = 0;

    this.set('removeOwner', () => {
      removeActionCallCount++;
      assert.equal(removeActionCallCount, 1, 'action is called once');
    });
    this.set('confirmSuggestedOwner', noop);
    this.set('author', { owner: confirmedOwner });
    this.set('commonOwners', commonOwners);

    await render(
      hbs`{{dataset-author confirmSuggestedOwner=confirmSuggestedOwner removeOwner=removeOwner owner=author commonOwners=commonOwners}}`
    );

    assert.equal(removeActionCallCount, 0, 'action is not called on render');

    triggerEvent('.remove-dataset-author', 'click');
  });

  test('triggers the confirmSuggestedOwner action when invoked', async function(assert) {
    assert.expect(2);
    let confirmSuggestedOwnerActionCallCount = 0;

    this.set('removeOwner', noop);
    this.set('confirmSuggestedOwner', () => {
      confirmSuggestedOwnerActionCallCount++;
      assert.equal(confirmSuggestedOwnerActionCallCount, 1, 'action is called once');
    });
    this.set('author', { owner: suggestedOwner });
    this.set('commonOwners', commonOwners);

    await render(
      hbs`{{dataset-author confirmSuggestedOwner=confirmSuggestedOwner removeOwner=removeOwner owner=author commonOwners=commonOwners}}`
    );

    assert.equal(confirmSuggestedOwnerActionCallCount, 0, 'action is not called on render');

    triggerEvent('.confirm-suggested-dataset-author', 'click');
  });

  test('triggers the updateOwnerType action when invoked', async function(assert) {
    assert.expect(2);

    this.set('removeOwner', noop);
    this.set('confirmSuggestedOwner', noop);
    this.set('updateOwnerType', (owner, type) => {
      assert.ok(confirmedOwner === owner, 'updateOwnerType action is invoked correct owner reference');
      assert.equal(type, confirmedOwner.type, 'updateOwnerType action is invoked with selected type');
    });
    this.set('author', { owner: confirmedOwner });
    this.set('commonOwners', commonOwners);
    this.set('ownerTypes', ownerTypes);

    await render(
      hbs`{{dataset-author confirmSuggestedOwner=confirmSuggestedOwner removeOwner=removeOwner owner=author commonOwners=commonOwners updateOwnerType=updateOwnerType ownerTypes=ownerTypes}}`
    );

    triggerEvent('select', 'change');
  });
});
