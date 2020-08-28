import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | custom-search-result-property-component/tag', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders correctly when titleize and modifiers are present', async function(assert): Promise<void> {
    const mockOptions = {
      styleModifier: 'class1',
      titleize: true
    };
    this.set('value', 'testValue');
    this.set('options', mockOptions);
    await render(
      hbs`<Search::CustomSearchResultPropertyComponent::Tag @value={{this.value}} @options={{this.options}} />`
    );

    assert.ok(this.element, 'Initial render is without errors');
    assert.equal(findAll('.nacho-pill').length, 1, 'Renders a pill with the right class1');
    assert.equal(findAll('.nacho-pill--small').length, 1, 'Renders a pill with the right class2');
    assert.equal(findAll('.entity-pillclass1').length, 1, 'Renders a pill with the right class3');
    assert.equal(this.element.textContent && this.element.textContent.trim(), 'Testvalue');
  });

  test('it renders correctly when titleize and modifiers are absent', async function(assert): Promise<void> {
    this.set('value', 'testValue');
    this.set('options', {});
    await render(
      hbs`<Search::CustomSearchResultPropertyComponent::Tag @value={{this.value}} @options={{this.options}} />`
    );

    assert.ok(this.element, 'Initial render is without errors');
    assert.equal(findAll('.nacho-pill').length, 1, 'Renders a pill with the right class');
    assert.equal(findAll('.nacho-pill--small').length, 1, 'Renders a pill with the right class');
    assert.equal(findAll('.class1').length, 0, 'Renders a pill with the right class');
    assert.equal(this.element.textContent && this.element.textContent.trim(), 'testValue');
  });
});
