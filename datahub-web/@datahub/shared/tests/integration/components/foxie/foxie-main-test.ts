import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { stubService } from '@datahub/utils/test-helpers/stub-service';

module('Integration | Component | foxie/foxie-main', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    stubService('configurator', {
      getConfig(): boolean {
        return true;
      }
    });
    await render(hbs`<Foxie::FoxieMain />`);
    assert.ok(this.element, 'Initial render is without errors');
  });
});
