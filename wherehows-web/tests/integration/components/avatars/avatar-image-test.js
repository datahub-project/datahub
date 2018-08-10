import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import avatars from 'wherehows-web/mirage/fixtures/avatars';

const avatar = avatars[0];

module('Integration | Component | avatars/avatar image', function(hooks) {
  setupRenderingTest(hooks);

  test('avatar-image render', async function(assert) {
    const expectedClassName = '.avatar';
    let avatarImage;

    await render(hbs`{{avatars/avatar-image}}`);

    avatarImage = find(expectedClassName);
    assert.ok(avatarImage, 'it renders with expected className');
    assert.equal(avatarImage.tagName.toLowerCase(), 'img', 'it renders with expected element');

    this.set('avatar', avatar);
    await render(hbs`{{avatars/avatar-image avatar=avatar}}`);

    avatarImage = find(expectedClassName);
    assert.equal(avatarImage.getAttribute('alt'), avatar.name, 'alt attribute is bound to avatar name');
    assert.equal(avatarImage.getAttribute('src'), avatar.imageUrl, 'src attribute is bound to avatar imageUrl');
  });
});
