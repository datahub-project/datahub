import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

// Helps us convert to rgb to compare to window computed styles for our component
function hexToRgb(
  hex: string
): {
  r: number;
  g: number;
  b: number;
  asString(): string;
} {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex) || [];
  return {
    r: parseInt(result[1], 16),
    g: parseInt(result[2], 16),
    b: parseInt(result[3], 16),
    asString(): string {
      return `rgb(${this.r}, ${this.g}, ${this.b})`;
    }
  };
}

module('Unit | Styles Testing', function(hooks) {
  setupRenderingTest(hooks);

  test('function tests', async function(assert) {
    await render(hbs`<div id="color-div" class="test-color"></div>`);

    const expectedHex = '#665ed0';
    let myEl = find('#color-div');

    assert.equal(
      myEl && window.getComputedStyle(myEl).color,
      hexToRgb(expectedHex).asString(),
      'get-color function adds style as expected'
    );

    await render(hbs`<div id="spacing-div" class="test-item-spacing"></div>`);

    myEl = find('#spacing-div');

    assert.equal(
      myEl && window.getComputedStyle(myEl).marginLeft,
      '24px',
      'item-spacing function adds style as expected'
    );
  });

  test('mixin tests', async function(assert) {
    await render(hbs`<div id="nacho-container-div" class="test-nacho-container"></div>`);

    const myEl = find('#nacho-container-div');

    assert.equal(
      myEl && window.getComputedStyle(myEl).borderRadius,
      '2px',
      'nacho-container mixin was included as expected'
    );
  });
});
