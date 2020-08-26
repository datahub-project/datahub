import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll, triggerEvent } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { TestContext } from 'ember-test-helpers';

module('Integration | Component | nacho-pwr-lookup', function(hooks) {
  setupRenderingTest(hooks);

  const autosuggestionClass = '.nacho-pwr-lookup__auto-suggestion__input';
  const typeaheadContainerClass = '.nacho-pwr-lookup__typeahead-container';
  const typeaheadTriggerClass = `${typeaheadContainerClass} .ember-power-select-typeahead-trigger`;
  const typeaheadInputClass = `${typeaheadTriggerClass} .ember-power-select-typeahead-input`;

  hooks.beforeEach(function(this: TestContext) {
    this.setProperties({
      confirmResult() {},
      searchResolver() {}
    });
  });

  test('it renders', async function(assert) {
    await render(hbs`{{nacho-pwr-lookup confirmResult=confirmResult searchResolver=searchResolver}}`);
    assert.ok(this.element, 'Initial render is without errors');
    assert.equal(findAll(autosuggestionClass).length, 1, 'Renders suggestion component');
    assert.equal(findAll(typeaheadInputClass).length, 1, 'Renders typeahead input component');
  });

  test('it properly triggers the search resolver action', async function(assert) {
    let searchResolverActionCallCount = 0;
    this.set('searchResolver', () => {
      searchResolverActionCallCount++;
      assert.equal(searchResolverActionCallCount, 1, 'searchResolver action is invoked when triggered');
    });

    await render(hbs`{{nacho-pwr-lookup searchResolver=searchResolver confirmResult=confirmResult}}`);

    assert.equal(searchResolverActionCallCount, 0, 'searchResolver action is not invoked on instantiation');
    triggerEvent(typeaheadInputClass, 'Pikachu');
  });
});
