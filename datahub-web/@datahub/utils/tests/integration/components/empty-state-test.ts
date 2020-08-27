import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | empty-state', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    await render(hbs`<EmptyState />`);

    assert.equal(this.element.textContent && this.element.textContent.trim(), 'No data found');

    await render(hbs`
      <EmptyState>
        template block text
      </EmptyState>
    `);

    assert.equal(this.element.textContent && this.element.textContent.trim(), 'template block text');
  });

  test('it renders a heading', async function(assert): Promise<void> {
    const heading = 'Not found!';
    assert.expect(1);

    this.set('heading', heading);

    await render(hbs`<EmptyState @heading={{heading}} />`);

    assert.equal(this.element.textContent && this.element.textContent.trim(), heading, 'shows the heading text');
  });

  test('it renders a subheading', async function(assert): Promise<void> {
    const subHeading = 'We could not find any results.';
    assert.expect(1);

    this.set('subHeading', subHeading);

    await render(hbs`<EmptyState @subHead={{subHeading}} />`);

    const subHead = this.element.querySelector('.empty-state__sub-head');
    assert.equal(subHead && subHead.textContent && subHead.textContent.trim(), subHeading, 'shows the subheading text');
  });
});
