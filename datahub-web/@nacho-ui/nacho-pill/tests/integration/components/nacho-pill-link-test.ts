import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | nacho-pill-link', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.set('text', 'Luke Skywalker');
    await render(hbs`{{nacho-pill-link linkTo="demo" text=text}}`);

    assert.equal(this.element.textContent?.trim(), this.get('text'), 'Renders correct link text');
    assert.equal(findAll('a').length, 1, 'Renders a single link component');
  });
});
