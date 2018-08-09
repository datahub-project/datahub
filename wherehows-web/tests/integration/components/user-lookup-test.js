import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { triggerEvent } from 'ember-native-dom-helpers';

module('Integration | Component | user lookup', function(hooks) {
  setupRenderingTest(hooks);
  const userLookupTypeahead = '.user-lookup__input';

  test('it renders', async function(assert) {
    await render(hbs`{{user-lookup}}`);

    assert.equal(document.querySelector(userLookupTypeahead).tagName, 'INPUT');
  });

  test('it triggers the findUser action', async function(assert) {
    let findUserActionCallCount = 0;
    this.set('findUser', () => {
      findUserActionCallCount++;
      assert.equal(findUserActionCallCount, 1, 'findUser action is invoked when triggered');
    });

    await render(hbs`{{user-lookup didFindUser=findUser}}`);
    assert.equal(findUserActionCallCount, 0, 'findUser action is not invoked on instantiation');

    triggerEvent(userLookupTypeahead, 'input');
  });
});
