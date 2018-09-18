import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

import { noop } from 'wherehows-web/utils/helpers/functions';
import { OwnerType, OwnerSource } from 'wherehows-web/utils/api/datasets/owners';
import owners from 'wherehows-web/mirage/fixtures/owners';
import userStub from 'wherehows-web/tests/stubs/services/current-user';
import { minRequiredConfirmedOwners } from 'wherehows-web/constants/datasets/owner';

const [confirmedOwner] = owners;
const ownerTypes = Object.values(OwnerType);
const saveButtonSelector = '.dataset-authors-save';

module('Integration | Component | dataset authors', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    this.owner.register('service:current-user', userStub);

    this['current-user'] = this.owner.lookup('service:current-user');
  });

  test('it renders', async function(assert) {
    assert.expect(1);
    this.set('owners', owners);
    this.set('ownerTypes', ownerTypes);
    this.set('saveOwnerChanges', noop);
    await render(hbs`{{dataset-authors owners=owners ownerTypes=ownerTypes save=(action saveOwnerChanges)}}`);

    assert.equal(findAll('.dataset-author').length, 2, 'expected two dataset author components to be rendered');
  });

  test('it should remove an owner when removeOwner is invoked', async function(assert) {
    assert.expect(1);
    this.set('owners', [confirmedOwner]);
    this.set('ownerTypes', ownerTypes);
    this.set('saveOwnerChanges', noop);
    await render(hbs`{{dataset-authors owners=owners ownerTypes=ownerTypes save=(action saveOwnerChanges)}}`);
    await click('.remove-dataset-author');

    assert.equal(this.get('owners').length, 0);
  });

  test('it should update a suggested owner to confirmed', async function(assert) {
    assert.expect(3);
    const [owner, suggestedOwner] = owners;
    const resolvedOwners = [owner];
    const suggestedOwners = [suggestedOwner];

    const initialLength = resolvedOwners.length;
    let userName, confirmedOwner;

    this.set('owners', resolvedOwners);
    this.set('ownerTypes', ownerTypes);
    this.set('saveOwnerChanges', noop);
    this.set('suggestedOwners', suggestedOwners);
    await render(
      hbs`{{dataset-authors owners=owners suggestedOwners=suggestedOwners ownerTypes=ownerTypes save=(action saveOwnerChanges)}}`
    );

    assert.equal(
      this.get('owners.length'),
      initialLength,
      `the list of owners is ${initialLength} before adding confirmed owner`
    );

    await click('.dataset-authors-suggested__info__trigger');
    await click('.suggested-owner-card__owner-info__add button');

    userName = this.get('current-user.currentUser.userName');
    confirmedOwner = this.get('owners').findBy('confirmedBy', userName);

    assert.equal(this.get('owners.length'), initialLength + 1, 'the list of owner contains one more new owner');
    assert.equal(confirmedOwner.source, OwnerSource.Ui, 'contains a new owner with ui source');
  });

  test('it should disable the save button when confirmedOwners is less than required minimum', async function(assert) {
    assert.expect(2);
    this.set('owners', [confirmedOwner]);
    this.set('ownerTypes', ownerTypes);
    this.set('saveOwnerChanges', noop);
    await render(hbs`{{dataset-authors owners=owners ownerTypes=ownerTypes save=(action saveOwnerChanges)}}`);

    const isDisabled = document.querySelector(saveButtonSelector).disabled;

    assert.ok(
      this.get('owners').length < minRequiredConfirmedOwners,
      `owners is less than the minimum required ${minRequiredConfirmedOwners}`
    );
    assert.equal(isDisabled, true, 'save button interaction is disabled');
  });

  test('it should invoke the external save action on save', async function(assert) {
    assert.expect(3);
    const confirmedOwners = [confirmedOwner, confirmedOwner];

    this.set('owners', []);
    this.set('ownerTypes', ownerTypes);
    this.set('saveOwnerChanges', owners => {
      assert.ok(owners === this.get('owners'), 'expect the list of owners is passed into the save action');
    });
    await render(hbs`{{dataset-authors owners=owners ownerTypes=ownerTypes save=(action saveOwnerChanges)}}`);

    this.set('owners', confirmedOwners);

    assert.equal(
      document.querySelector('.dataset-authors-save').disabled,
      false,
      'expect save button to not be disabled'
    );
    await click(saveButtonSelector);

    assert.equal(this.get('owners'), confirmedOwners, 'expect the same owners list as the passed in confirmed owners');
  });
});
