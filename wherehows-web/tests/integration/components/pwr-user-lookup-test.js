import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { triggerEvent } from 'ember-native-dom-helpers';

moduleForComponent('pwr-user-lookup', 'Integration | Component | pwr user lookup', {
  integration: true
});

const autosuggestionClass = '.pwr-user-lookup__auto-suggestion__input';
const typeaheadContainerClass = '.pwr-user-lookup__typeahead-container';
const typeaheadTriggerClass = `${typeaheadContainerClass} .ember-power-select-typeahead-trigger`;
const typeaheadInputClass = `${typeaheadTriggerClass} .ember-power-select-typeahead-input`;

test('it renders', function(assert) {
  this.render(hbs`{{pwr-user-lookup}}`);
  assert.ok(this.$(), 'Renders without errors');
  assert.equal(this.$(autosuggestionClass).length, 1, 'Renders suggestion component');
  assert.equal(this.$(typeaheadInputClass).length, 1, 'Renders typeahead input component');
});

test('it properly triggers the findUser action', function(assert) {
  let findUserActionCallCount = 0;
  this.set('findUser', () => {
    findUserActionCallCount++;
    assert.equal(findUserActionCallCount, 1, 'findUser action is invoked when triggered');
  });

  this.render(hbs`{{pwr-user-lookup didFindUser=findUser}}`);

  assert.equal(findUserActionCallCount, 0, 'findUser action is not invoked on instantiation');
  triggerEvent(typeaheadInputClass, 'Pikachu');
});
