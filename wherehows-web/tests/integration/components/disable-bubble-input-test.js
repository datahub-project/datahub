import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { triggerEvent } from 'ember-native-dom-helpers';

moduleForComponent('disable-bubble-input', 'Integration | Component | disable bubble input', {
  integration: true
});

test('it renders', function(assert) {
  this.render(hbs`{{disable-bubble-input id='test-disable'}}`);

  assert.equal(document.querySelector('#test-disable').tagName, 'INPUT');
});

test('it invokes the click action on click', function(assert) {
  const inputClickAction = () => {
    assert.ok(true, 'disable input click action invoked');
  };
  this.set('inputClickAction', inputClickAction);

  this.render(hbs`{{disable-bubble-input click=inputClickAction}}`);

  triggerEvent('input[type=text]', 'click');
});
