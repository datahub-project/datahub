import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import AvatarMainContainer from '@datahub/shared/components/avatar/containers/avatar-main';
import { Avatar } from '@datahub/shared/modules/avatars/avatar';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

module('Integration | Component | avatar/containers/avatar-main', function(hooks) {
  setupRenderingTest(hooks);
  const sampleUrn = 'urn:li:corpuser:pikachu';

  test('it works as expected with a urn', async function(assert) {
    this.set('urn', sampleUrn);

    const component = await getRenderedComponent({
      componentName: 'avatar/containers/avatar-main',
      ComponentToRender: AvatarMainContainer,
      testContext: this,
      template: hbs`<Avatar::Containers::AvatarMain @urn={{this.urn}} />`
    });

    assert.equal(component.urns[0], sampleUrn, 'Gets urns correctly when given urn');
    assert.equal(component.avatars?.length, 1, 'Gives the correct number of avatars');

    const firstAvatar = (component.avatars as Array<Avatar>)[0];
    assert.ok(firstAvatar instanceof Avatar, 'Creates an expected avatar instance');
    assert.equal(firstAvatar.urn, sampleUrn);
  });

  test('it works as expected with an entity', async function(assert) {
    const sampleEntity = new PersonEntity(sampleUrn);
    this.set('entity', sampleEntity);

    const secondComponent = await getRenderedComponent({
      componentName: 'avatar/containers/avatar-main',
      ComponentToRender: AvatarMainContainer,
      testContext: this,
      template: hbs`<Avatar::Containers::AvatarMain @entity={{this.entity}} />`
    });

    const secondAvatar = (secondComponent.avatars as Array<Avatar>)[0];
    assert.ok(secondAvatar instanceof Avatar, 'Creates an expected avatar instance when given an entity');
    assert.equal(secondAvatar.urn, sampleUrn, 'Given avatar from entity correctly reads underlying entity');
  });
});
