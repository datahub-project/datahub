import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { get } from '@ember/object';

module('Integration | Helper | find-in-array', function(hooks) {
  setupRenderingTest(hooks);

  test('it works for primitive values', async function(assert) {
    this.setProperties({
      sampleArray: ['pikachu', 'charmander', 'squirtle', 'bulbasaur'],
      inputA: 'pikachu',
      inputB: 'ash ketchum'
    });

    await render(hbs`{{find-in-array sampleArray inputA}}`);
    assert.equal(
      this.element.textContent!.trim(),
      get(this as any, 'inputA'),
      'Helper returns primitive item if found in array'
    );

    await render(hbs`{{find-in-array sampleArray inputB}}`);
    assert.equal(this.element.textContent!.trim(), '', 'Helper returns nothing if primitive not found in array');
  });

  test('it works for primitive values', async function(assert) {
    this.setProperties({
      sampleArray: [{ id: 25, name: 'pikachu' }, { id: 133, name: 'eevee' }],
      inputA: (item: any) => item.name === 'eevee',
      inputB: (item: any) => item.id === 7
    });

    await render(hbs`{{get (find-in-array sampleArray inputA) "name"}}`);
    assert.equal(this.element.textContent!.trim(), 'eevee', 'Helper returns the proper object if found in array');

    await render(hbs`{{get (find-in-array sampleArray inputB) "id"}}`);
    assert.equal(
      this.element.textContent!.trim(),
      '',
      'Helper returns nothing if predicate condition not found in array'
    );
  });
});
