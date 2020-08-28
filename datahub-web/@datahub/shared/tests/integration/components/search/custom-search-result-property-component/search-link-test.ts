import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | custom-search-result-property-component/link', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    const entity = {
      entityLink: {
        link: {
          model: ['some model'],
          queryParams: {},
          route: '',
          text: ''
        }
      }
    };
    const options = {
      linkProperty: 'entityLink',
      text: 'View in a nice place'
    };

    this.set('entity', entity);
    this.set('options', options);
    await render(
      hbs`<Search::CustomSearchResultPropertyComponent::Link @entity={{this.entity}} @options={{this.options}}/>`
    );

    assert.equal(this.element.textContent && this.element.textContent.trim(), 'View in a nice place');
  });
});
