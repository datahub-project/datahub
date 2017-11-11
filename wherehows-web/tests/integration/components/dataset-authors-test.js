import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { run } from '@ember/runloop';

import noop from 'wherehows-web/utils/noop';
import { OwnerType, OwnerSource } from 'wherehows-web/utils/api/datasets/owners';
import owners from 'wherehows-web/mirage/fixtures/owners';

import userStub from 'wherehows-web/tests/stubs/services/current-user';

const [confirmedOwner] = owners;
const ownerTypes = Object.values(OwnerType);

moduleForComponent('dataset-authors', 'Integration | Component | dataset authors', {
  integration: true,

  beforeEach() {
    this.register('service:current-user', userStub);

    this.inject.service('current-user');
  }
});

test('it renders', function(assert) {
  assert.expect(1);
  this.set('owners', owners);
  this.set('ownerTypes', ownerTypes);
  this.set('saveOwnerChanges', noop);
  this.render(hbs`{{dataset-authors owners=owners ownerTypes=ownerTypes save=(action saveOwnerChanges)}}`);

  assert.equal(this.$('.dataset-author').length, 2, 'expected two dataset author components to be rendered');
});

test('it should remove an owner when removeOwner is invoked', function(assert) {
  assert.expect(1);
  this.set('owners', [confirmedOwner]);
  this.set('ownerTypes', ownerTypes);
  this.set('saveOwnerChanges', noop);
  this.render(hbs`{{dataset-authors owners=owners ownerTypes=ownerTypes save=(action saveOwnerChanges)}}`);

  run(() => {
    document.querySelector('.remove-dataset-author').click();
  });

  assert.equal(this.get('owners').length, 0);
});

test('it should update a suggested owner to confirmed', function(assert) {
  assert.expect(3);

  const initialLength = owners.length;
  this.set('owners', owners);
  this.set('ownerTypes', ownerTypes);
  this.set('saveOwnerChanges', noop);
  this.render(hbs`{{dataset-authors owners=owners ownerTypes=ownerTypes save=(action saveOwnerChanges)}}`);

  assert.equal(
    this.get('owners.length'),
    initialLength,
    `the list of owners is ${initialLength} before adding confirmed owner`
  );
  run(() => {
    document.querySelector('.confirm-suggested-dataset-author').click();
  });

  assert.equal(this.get('owners.length'), initialLength + 1, 'the list of owner contains one more new owner');
  assert.equal(this.get('owners.lastObject.source'), OwnerSource.Ui, 'contains a new owner with ui source');
});

test('it should invoke the external save action on save', function(assert) {
  assert.expect(2);
  this.set('owners', [confirmedOwner]);
  this.set('ownerTypes', ownerTypes);
  this.set('saveOwnerChanges', owners => {
    assert.ok(owners === this.get('owners'), 'the list of owners is passed into the save action');
  });
  this.render(hbs`{{dataset-authors owners=owners ownerTypes=ownerTypes save=(action saveOwnerChanges)}}`);

  run(() => {
    document.querySelector('.dataset-authors-save').click();
  });

  assert.equal(this.get('owners').length, 1);
});
