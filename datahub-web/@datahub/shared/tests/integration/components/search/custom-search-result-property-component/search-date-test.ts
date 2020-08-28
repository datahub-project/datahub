import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import moment from 'moment';

module('Integration | Component | custom-search-result-property-component/date', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    this.setProperties({
      value:
        moment('2018-11-28 18:42:00')
          .toDate()
          .valueOf() / 1000,
      options: {
        inSeconds: true,
        format: 'MM/DD/YYYY, hh:mm a'
      }
    });
    await render(hbs`<Search::CustomSearchResultPropertyComponent::Date @value={{value}} @options={{options}}/>`);

    assert.equal(this.element.textContent && this.element.textContent.trim(), '11/28/2018, 06:42 pm');
  });
});
