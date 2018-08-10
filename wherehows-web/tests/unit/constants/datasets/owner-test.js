import { module, test } from 'qunit';
import { updateOwner, confirmOwner, ownerAlreadyExists } from 'wherehows-web/constants/datasets/owner';
import owners from 'wherehows-web/mirage/fixtures/owners';

let testOwners;
let testOwner;

module('Unit | Constants | constants/datasets/owner', function(hooks) {
  hooks.beforeEach(function() {
    testOwners = JSON.parse(JSON.stringify(owners));
    testOwner = testOwners[0];
  });

  test('updateOwner behaves as expected', function(assert) {
    assert.expect(6);

    assert.ok(typeof updateOwner === 'function', 'exported property updateOwner is a function');

    assert.ok(
      typeof updateOwner() === 'undefined',
      'invoking update owner without arguments is undefined and does not throw'
    );

    const testOwnerConfirmedBy = 'testOwner';
    let result = updateOwner(testOwners, testOwner, 'confirmedBy', testOwnerConfirmedBy);
    let [updatedTestOwner] = result;

    assert.ok(Array.isArray(result), 'invoking updateOwner with expected arguments is an array');

    assert.ok(testOwner !== updatedTestOwner, 'updateOwner does not mutate the original owner reference');

    assert.equal(
      updatedTestOwner.confirmedBy,
      testOwnerConfirmedBy,
      'correctly updates a property on the owner object when a single property is specified'
    );

    testOwner = testOwners[0];
    result = updateOwner(testOwners, testOwner, { confirmedBy: null });
    updatedTestOwner = result[0];

    assert.equal(
      updatedTestOwner.confirmedBy,
      null,
      'correctly updates properties on the owner object when a hash of properties is provided'
    );
  });

  test('confirmOwner behaves as expected', function(assert) {
    assert.ok(typeof confirmOwner === 'function', 'exported property confirmOwner is a function');
    confirmOwner(testOwner);

    assert.ok(
      testOwner.confirmedBy === null,
      'invoking confirmOwner without a second argument set the confirmedBy attr to null'
    );
    confirmOwner(testOwner, 'testOwner');
    assert.ok(
      testOwner.confirmedBy === 'testOwner',
      'confirmOwner correctly updates to confirmedBy attr when invoked with a string arguments'
    );
  });

  test('ownerAlreadyExists behaves as expected', function(assert) {
    const [{ userName, source }] = testOwners;

    assert.ok(typeof ownerAlreadyExists === 'function', 'exported property ownerAlreadyExists is a function');

    assert.ok(
      ownerAlreadyExists(testOwners, { userName, source }),
      'ownerAlreadyExists correctly asserts that an owner with userName, source combination already exists'
    );

    assert.notOk(
      ownerAlreadyExists(testOwners, { userName }),
      'ownerAlreadyExists does not assert an owner exists when source is not present'
    );
    assert.notOk(
      ownerAlreadyExists(testOwners, { source }),
      'ownerAlreadyExists does not assert an owner exists when userName is not present'
    );
  });
});
