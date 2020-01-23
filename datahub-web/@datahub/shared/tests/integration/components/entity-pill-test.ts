import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | entity-pill', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders correctly when titleize and modifiers are present', async function(assert) {
    const mockField = {
      component: {
        attrs: {
          styleModifier: 'class1',
          titleize: true
        }
      }
    };
    this.set('value', 'testValue');
    this.set('field', mockField);
    await render(hbs`{{entity-pill value=value field=field }}`);

    assert.ok(this.element, 'Initial render is without errors');
    assert.equal(findAll('.nacho-pill').length, 1, 'Renders a pill with the right class1');
    assert.equal(findAll('.nacho-pill--small').length, 1, 'Renders a pill with the right class2');
    assert.equal(findAll('.entity-pillclass1').length, 1, 'Renders a pill with the right class3');
    assert.equal(this.element.textContent!.trim(), 'Testvalue');
  });

  test('it renders correctly when titleize and modifiers are absent', async function(assert) {
    this.set('value', 'testValue');
    this.set('field', {});
    await render(hbs`{{entity-pill value=value field=field }}`);

    assert.ok(this.element, 'Initial render is without errors');
    assert.equal(findAll('.nacho-pill').length, 1, 'Renders a pill with the right class');
    assert.equal(findAll('.nacho-pill--small').length, 1, 'Renders a pill with the right class');
    assert.equal(findAll('.class1').length, 0, 'Renders a pill with the right class');
    assert.equal(this.element.textContent!.trim(), 'testValue');
  });

  test('it renders correctly when styleModifier is passed directly', async function(assert) {
    this.set('value', 'testValue');
    this.set('styleModifier', '--advanced-style');
    await render(hbs`{{entity-pill value=value styleModifier=styleModifier }}`);

    assert.ok(this.element, 'Initial render is without errors');
    assert.equal(findAll('.nacho-pill').length, 1, 'Renders a pill with the right class1');
    assert.equal(findAll('.nacho-pill--small').length, 1, 'Renders a pill with the right class2');
    assert.equal(findAll('.entity-pill--advanced-style').length, 1, 'Renders a pill with the right class3');
    assert.equal(this.element.textContent!.trim(), 'testValue');
  });
});
