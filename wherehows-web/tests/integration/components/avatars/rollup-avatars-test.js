import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { find, click } from 'ember-native-dom-helpers';

moduleForComponent('avatars/rollup-avatars', 'Integration | Component | avatars/rollup avatars', {
  integration: true
});

test('rollup-avatars rendering', function(assert) {
  this.render(hbs`
    {{#avatars/rollup-avatars}}
    {{/avatars/rollup-avatars}}
  `);

  assert.ok(find('.avatar-rollup'), 'renders the element with expected className');
  assert.notOk(find('.avatar-detail-container'), 'modal element is not rendered');
});

test('rollup-avatars rendering modal', async function(assert) {
  const avatarType = 'Avatar Rollup';
  this.set('avatarType', avatarType);

  this.render(hbs`
    {{#avatars/rollup-avatars avatarType=avatarType}}
    {{/avatars/rollup-avatars}}
  `);

  await click('.avatar-rollup');

  assert.ok(find('.avatar-detail-container'), 'modal element is rendered');
  assert.ok(find('.avatars-detail-modal__header').textContent, avatarType, 'it renders the avatar detail header');
});
