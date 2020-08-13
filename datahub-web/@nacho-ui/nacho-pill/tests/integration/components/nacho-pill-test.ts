import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | nacho-pill', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.set('text', 'Luke Skywalker');
    await render(hbs`<NachoPill @text={{text}} />`);

    assert.equal((this.element.textContent as string).trim(), this.get('text'), 'Renders correct link text');
    assert.equal(findAll('.nacho-pill').length, 1, 'Renders a pill component');
    assert.equal(findAll('.nacho-pill--neutral').length, 1, 'Renders expected class for undefined state');

    this.set('state', 'alert');
    await render(hbs`<NachoPill @text={{text}} @state={{state}} />`);

    assert.equal(findAll('.nacho-pill--alert').length, 1, 'Renders expected class for passed in state');
    assert.equal(findAll('.nacho-pill--neutral').length, 0, 'Does not render class if not assigned');

    this.set('state', 'light-neutral');
    await render(hbs`<NachoPill @text={{text}} @state={{state}} />`);

    assert.equal(findAll('.nacho-pill--light-neutral').length, 1, 'Renders expected class for passed in state');
    assert.equal(findAll('.nacho-pill--neutral').length, 0, 'Does not render class if not assigned');

    this.set('state', 'good-inverse');
    await render(hbs`<NachoPill @text={{text}} @state={{state}} />`);

    assert.equal(findAll('.nacho-pill--good-inverse').length, 1, 'Renders expected class for passed in state');
    assert.equal(findAll('.nacho-pill--neutral').length, 0, 'Does not render class if not assigned');
  });
});
