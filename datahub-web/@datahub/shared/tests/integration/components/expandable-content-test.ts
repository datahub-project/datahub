import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { click, render, find, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | expandable-content', function(hooks): void {
  setupRenderingTest(hooks);

  const expandableContent = '.expandable-content';

  test('it renders', async function(assert): Promise<void> {
    await render(hbs`<ExpandableContent @title="Pikachu">
                       <div class="inside-expanded-content">
                         <img src="https://wi-images.condecdn.net/image/5NYM2eyGW4K/crop/810/f/download.jpg" />
                       </div>
                     </ExpandableContent>`);

    assert.ok(this.element, 'Renders without errors');
    assert.equal(findAll(expandableContent).length, 1, 'Renders one expandable content');
    assert.equal(
      (find(expandableContent) as HTMLElement).textContent!.trim(),
      'Pikachu',
      'Renders the correct title for the expandable content'
    );

    assert.equal(
      findAll('.inside-expanded-content').length,
      0,
      'Does not render expanded content while isExpanded === false'
    );
  });

  test('it expands as expected', async function(assert): Promise<void> {
    await render(hbs`<ExpandableContent @title="Pikachu">
                       <div class="inside-expanded-content">
                         <img src="https://wi-images.condecdn.net/image/5NYM2eyGW4K/crop/810/f/download.jpg" />
                       </div>
                     </ExpandableContent>`);

    assert.equal(
      findAll('.inside-expanded-content').length,
      0,
      'Does not render expanded content while isExpanded === false'
    );

    await click(`${expandableContent}__button`);

    assert.equal(findAll('.inside-expanded-content').length, 1, 'Shows expanded content when we click the trigger');
  });
});
