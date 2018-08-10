import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | avatars/rollup avatars', function(hooks) {
  setupRenderingTest(hooks);

  test('rollup-avatars rendering', async function(assert) {
    await render(hbs`
      {{#avatars/rollup-avatars}}
      {{/avatars/rollup-avatars}}
    `);

    assert.ok(find('.avatar-rollup'), 'renders the element with expected className');
    assert.notOk(find('.avatar-detail-container'), 'modal element is not rendered');
  });

  test('rollup-avatars rendering modal', async function(assert) {
    const avatarType = 'Avatar Rollup';
    this.set('avatarType', avatarType);

    await render(hbs`
      {{#avatars/rollup-avatars avatarType=avatarType}}
      {{/avatars/rollup-avatars}}
    `);

    await click('.avatar-rollup');

    assert.ok(find('.avatar-detail-container'), 'modal element is rendered');
    assert.ok(find('.avatars-detail-modal__header').textContent, avatarType, 'it renders the avatar detail header');
  });
});
