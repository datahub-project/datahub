import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { triggerEvent } from 'ember-native-dom-helpers';

module('Integration | Component | pwr user lookup', function(hooks) {
  setupRenderingTest(hooks);

  const autosuggestionClass = '.pwr-user-lookup__auto-suggestion__input';
  const typeaheadContainerClass = '.pwr-user-lookup__typeahead-container';
  const typeaheadTriggerClass = `${typeaheadContainerClass} .ember-power-select-typeahead-trigger`;
  const typeaheadInputClass = `${typeaheadTriggerClass} .ember-power-select-typeahead-input`;

  test('it renders', async function(assert) {
    await render(hbs`{{pwr-user-lookup}}`);
    assert.ok(this.$(), 'Renders without errors');
    assert.equal(this.$(autosuggestionClass).length, 1, 'Renders suggestion component');
    assert.equal(this.$(typeaheadInputClass).length, 1, 'Renders typeahead input component');
  });

  test('it properly triggers the findUser action', async function(assert) {
    let findUserActionCallCount = 0;
    this.set('findUser', () => {
      findUserActionCallCount++;
      assert.equal(findUserActionCallCount, 1, 'findUser action is invoked when triggered');
    });

    await render(hbs`{{pwr-user-lookup didFindUser=findUser}}`);

    assert.equal(findUserActionCallCount, 0, 'findUser action is not invoked on instantiation');
    triggerEvent(typeaheadInputClass, 'Pikachu');
  });
});
