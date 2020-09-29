import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | entity-alert-banner', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    // TODO META-11234: Refactor entity alert banner to be generic
    await render(hbs`<EntityAlertBanner @configs={{hash appworx-deprecation=true}}/>`);

    assert
      .dom()
      .hasText(
        'This dataset will be deprecated by May 31st 2020 as part of a company wide horizontal initiative HI 316 . Once deprecated the dataset won’t available there after. Please visit the dataset list to view SOT (Source of Truth) replacement dataset options. For more information, including contact information, please view the wiki .'
      );
  });
});
