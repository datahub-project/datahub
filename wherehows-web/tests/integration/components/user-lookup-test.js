import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { triggerEvent } from 'ember-native-dom-helpers';

moduleForComponent('user-lookup', 'Integration | Component | user lookup', {
  integration: true
});
const userLookupTypeahead = '.user-lookup__input';

test('it renders', function(assert) {
  this.render(hbs`{{user-lookup}}`);

  assert.equal(document.querySelector(userLookupTypeahead).tagName, 'INPUT');
});

test('it triggers the findUser action', function(assert) {
  let findUserActionCallCount = 0;
  this.set('findUser', () => {
    findUserActionCallCount++;
    assert.equal(findUserActionCallCount, 1, 'findUser action is invoked when triggered');
  });

  this.render(hbs`{{user-lookup didFindUser=findUser}}`);
  assert.equal(findUserActionCallCount, 0, 'findUser action is not invoked on instantiation');

  triggerEvent(userLookupTypeahead, 'input');
});
