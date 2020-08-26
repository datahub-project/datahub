import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { baseAvatarNameComponentClass } from '@datahub/shared/components/avatar/avatar-name';
import { Avatar } from '@datahub/shared/modules/avatars/avatar';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

module('Integration | Component | avatar/avatar-name', function(hooks) {
  setupRenderingTest(hooks);

  const baseSelector = `.${baseAvatarNameComponentClass}`;

  test('it behaves as expected', async function(assert) {
    // Base case testing
    await render(hbs`<Avatar::AvatarName />`);
    assert.ok(this.element, 'Empty state does not throw an error');
    assert.dom(baseSelector).doesNotExist('Without an avatar does not render link');

    const myAvatar = new Avatar(new PersonEntity('urn:li:corpuser:pikachu'));
    this.set('avatar', myAvatar);

    await render(hbs`<Avatar::AvatarName @avatar={{this.avatar}}/>`);
    assert.dom(baseSelector).exists('Renders a proper avatar name');
    assert.dom(baseSelector).hasText('pikachu', 'Renders expected username');
  });
});
