import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { Avatar } from '@datahub/shared/modules/avatars/avatar';

module('Unit | Module | Avatar', function(hooks): void {
  setupTest(hooks);

  test('it works as intended', function(assert): void {
    const samplePerson = new PersonEntity('urn:li:corpuser:charmander');
    const sampleAvatar = new Avatar(samplePerson);

    assert.ok(sampleAvatar instanceof Avatar, 'Created the proper class');
    assert.equal(sampleAvatar.username, 'charmander', 'Reads the underlying entity properly');
  });
});
