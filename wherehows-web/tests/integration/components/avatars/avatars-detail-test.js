import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { noop } from 'wherehows-web/utils/helpers/functions';

module('Integration | Component | avatars/avatars detail', function(hooks) {
  setupRenderingTest(hooks);

  test('avatars detail render', async function(assert) {
    const header = 'Avatar Detail Header';

    this.set('header', header);
    this.set('onClose', noop);

    await render(hbs`
      {{#avatars/avatars-detail header=header onClose=onClose}}
      {{/avatars/avatars-detail}}
    `);

    assert.ok(find('.avatars-detail-modal'), 'it renders the element with expected className');
    assert.ok(find('.avatars-detail-modal__header').textContent, header, 'it renders the avatar detail header');
  });
});
