import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | empty state', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    // Set any properties with this.set('myProperty', 'value');
    // Handle any actions with this.on('myAction', function(val) { ... });

    await render(hbs`{{empty-state}}`);

    assert.equal(
      this.$()
        .text()
        .trim(),
      'No data found'
    );

    // Template block usage:
    await render(hbs`
      {{#empty-state}}
        template block text
      {{/empty-state}}
    `);

    assert.equal(
      this.$()
        .text()
        .trim(),
      'template block text'
    );
  });

  test('it renders a heading', async function(assert) {
    const heading = 'Not found!';
    assert.expect(1);

    this.set('heading', heading);

    await render(hbs`{{empty-state heading=heading}}`);

    assert.equal(
      this.$()
        .text()
        .trim(),
      heading,
      'shows the heading text'
    );
  });

  test('it renders a subheading', async function(assert) {
    const subHeading = 'We could not find any results.';
    assert.expect(1);

    this.set('subHeading', subHeading);

    await render(hbs`{{empty-state subHead=subHeading}}`);

    assert.equal(
      this.$('.empty-state__sub-head')
        .text()
        .trim(),
      subHeading,
      'shows the subheading text'
    );
  });
});
