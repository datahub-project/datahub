import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { stubService } from '@datahub/utils/test-helpers/stub-service';

module('Integration | Component | dynamic-components/wiki-link', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    stubService('configurator', {
      getConfig(): unknown {
        return { appHelp: 'https://www.serebii.net/pokedex-swsh/pikachu/' };
      }
    });

    const sampleRenderProps = {
      name: 'dynamic-components/wiki-link',
      options: { wikiKey: 'appHelp', className: 'pikachu-link' }
    };
    this.set('props', sampleRenderProps);

    await render(hbs`{{component this.props.name options=this.props.options}}`);
    assert.dom('.dynamic-wiki-link').exists('Renders the link as expected');
    assert.dom('.pikachu-link').exists('Renders with our custom class name');
    assert.dom('.pikachu-link').hasTextContaining('https://www.serebii.net/pokedex-swsh/pikachu/');
  });
});
