import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import 'qunit-dom';

module('Integration | Component | custom-search-result-property-component/icon', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    this.setProperties({
      value: true,
      options: {
        iconName: 'certificate'
      }
    });
    await render(
      hbs`<Search::CustomSearchResultPropertyComponent::Icon @value={{this.value}} @options={{this.options}} />`
    );

    assert.dom('.fa-certificate').exists();
  });

  test('it should not render', async function(assert): Promise<void> {
    this.setProperties({
      value: false,
      options: {
        iconName: 'certificate'
      }
    });
    await render(
      hbs`<Search::CustomSearchResultPropertyComponent::Icon @value={{this.value}} @options={{this.options}} />`
    );

    assert.dom('.fa-certificate').doesNotExist();
  });
});
