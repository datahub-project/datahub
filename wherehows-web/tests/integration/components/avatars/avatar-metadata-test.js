import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { find } from 'ember-native-dom-helpers';
import avatars from 'wherehows-web/mirage/fixtures/avatars';

const avatar = avatars[0];

moduleForComponent('avatars/avatar-metadata', 'Integration | Component | avatars/avatar metadata', {
  integration: true
});

test('avatar-metadata rendering', function(assert) {
  const expectedClassName = '.avatar-metadata';

  this.render(hbs`{{avatars/avatar-metadata}}`);

  assert.ok(find(expectedClassName), 'it renders with the expected className');

  this.set('avatar', avatar);
  this.render(hbs`
  {{#avatars/avatar-metadata avatar=avatar as |meta|}}
  {{meta.name}}{{meta.email}}{{meta.userName}}
  {{/avatars/avatar-metadata}}`);

  assert.equal(
    find(expectedClassName).textContent.trim(),
    `${avatar.name}${avatar.email}${avatar.userName}`,
    'contextual component yields expected avatar attributes'
  );
});
