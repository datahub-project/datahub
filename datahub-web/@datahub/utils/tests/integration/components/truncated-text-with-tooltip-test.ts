import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, triggerEvent } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | truncated-text-with-tooltip', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders a link if linkParams are passed in and text otherwise', async function(assert): Promise<void> {
    this.owner.lookup('router:main').setupRouter();

    // Test rendering of component when no link params are given

    const text = 'sampletext';

    this.set('text', text);

    await render(hbs`
      <TruncatedTextWithTooltip @text={{this.text}} />
    `);

    let component = document.querySelector('.truncated-text-with-tooltip') as HTMLElement;

    assert.equal(component.tagName, 'SPAN');
    assert.dom('.truncated-text-with-tooltip').hasText(text);

    // Test rendering of component expecting a link with link params given

    const linkParams = {
      title: 'title',
      text: 'text',
      route: 'route',
      queryParams: {
        query: 'value'
      }
    };

    this.set('linkParams', linkParams);

    await render(hbs`
      <TruncatedTextWithTooltip @text={{this.text}} @linkParams={{this.linkParams}} />
    `);

    component = document.querySelector('.truncated-text-with-tooltip') as HTMLElement;

    const link = component.getAttribute('href');

    assert.equal(component.tagName, 'A');
    assert.dom('.truncated-text-with-tooltip').hasText(text);
    assert.equal(link && decodeURIComponent(link), '/route?query=value');
  });

  test('it displays a tooltip only if hovering over a cell title that is truncated', async function(assert): Promise<
    void
  > {
    // Test rendering of tooltip if the text is short
    const shortText = 'short text';

    this.set('text', shortText);

    // Parent component must have a set width in order to detect overflow
    await render(hbs`
      <div style="width:200px">
        <TruncatedTextWithTooltip @text={{this.text}} />
      </div>
    `);

    let component = document.querySelector('.truncated-text-with-tooltip') as HTMLElement;

    await triggerEvent(component, 'mouseenter');

    assert.equal(document.getElementsByClassName('ember-tooltip').length, 0);

    // Test rendering of tooltip if the text is long
    const longText = 'really long sample text that gets truncated';

    this.set('text', longText);

    await render(hbs`
    <div style="width:200px">
      <TruncatedTextWithTooltip @text={{this.text}} />
    </div>
    `);

    component = document.querySelector('.truncated-text-with-tooltip') as HTMLElement;

    await triggerEvent(component, 'mouseenter');

    assert.equal(document.getElementsByClassName('ember-tooltip').length, 1);
  });
});
