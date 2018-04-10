import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import avatars from 'wherehows-web/mirage/fixtures/avatars';
import { find } from 'ember-native-dom-helpers';

const avatar = avatars[0];

moduleForComponent('avatars/avatar-image', 'Integration | Component | avatars/avatar image', {
  integration: true
});

test('avatar-image render', function(assert) {
  const expectedClassName = '.avatar';
  let avatarImage;

  this.render(hbs`{{avatars/avatar-image}}`);

  avatarImage = find(expectedClassName);
  assert.ok(avatarImage, 'it renders with expected className');
  assert.equal(avatarImage.tagName.toLowerCase(), 'img', 'it renders with expected element');

  this.set('avatar', avatar);
  this.render(hbs`{{avatars/avatar-image avatar=avatar}}`);

  avatarImage = find(expectedClassName);
  assert.equal(avatarImage.getAttribute('alt'), avatar.name, 'alt attribute is bound to avatar name');
  assert.equal(avatarImage.getAttribute('src'), avatar.imageUrl, 'src attribute is bound to avatar imageUrl');
});
