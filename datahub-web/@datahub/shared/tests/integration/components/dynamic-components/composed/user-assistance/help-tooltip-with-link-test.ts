import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, triggerEvent } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { stubService } from '@datahub/utils/test-helpers/stub-service';
module('Integration | Component | dynamic-components/composed/user-assistance/help-tooltip-with-link', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders and behaves as expected', async function(assert) {
    // Note: Composed dynamic components are compositions of other, tested based dynamic components.
    // Therefore, testing for a composed component will be relatively light
    stubService('configurator', {
      getConfig(): unknown {
        return { appHelp: 'https://www.serebii.net/pokedex-swsh/pikachu/' };
      }
    });

    const sampleRenderProps = {
      name: 'dynamic-components/composed/user-assistance/help-tooltip-with-link',
      options: {
        className: 'pikachu',
        text: 'Hello darkness my old friend',
        wikiKey: 'appHelp'
      }
    };
    this.set('props', sampleRenderProps);

    await render(hbs`{{component this.props.name options=this.props.options}}`);
    assert.dom('.dynamic-tooltip').exists('Renders a dynamic tooltip');
    assert.dom('.pikachu').exists('Gives us the correct custom class');

    await triggerEvent('.dynamic-tooltip', 'mouseenter');
    assert.dom('.dynamic-tooltip__content .dynamic-wiki-link').exists('Renders the nested link');
    assert.dom('.dynamic-tooltip__content').hasTextContaining('Hello darkness my old friend');
  });
});
