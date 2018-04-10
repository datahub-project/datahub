import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { find } from 'ember-native-dom-helpers';
import noop from 'wherehows-web/utils/noop';

moduleForComponent('avatars/avatars-detail', 'Integration | Component | avatars/avatars detail', {
  integration: true
});

test('avatars detail render', function(assert) {
  const header = 'Avatar Detail Header';

  this.set('header', header);
  this.set('onClose', noop);

  this.render(hbs`
    {{#avatars/avatars-detail header=header onClose=onClose}}
    {{/avatars/avatars-detail}}
  `);

  assert.ok(find('.avatars-detail-modal'), 'it renders the element with expected className');
  assert.ok(find('.avatars-detail-modal__header').textContent, header, 'it renders the avatar detail header');
});
