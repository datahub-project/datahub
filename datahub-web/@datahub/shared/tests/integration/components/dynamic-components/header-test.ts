import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, triggerEvent } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { stubService } from '@datahub/utils/test-helpers/stub-service';

module('Integration | Component | dynamic-components/header', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders and behaves as expected', async function(assert) {
    stubService('configurator', {
      getConfig(): unknown {
        return { appHelp: 'https://stardewvalleywiki.com/Stardew_Valley_Wiki' };
      }
    });
    const sampleRenderProps = {
      name: 'dynamic-components/header',
      options: {
        className: 'mining',
        title: 'Gotta mine em all!',
        contentComponents: [
          {
            name: 'dynamic-components/text',
            options: {
              text: 'Mining tutorial'
            }
          },
          {
            name: 'dynamic-components/tooltip',
            options: {
              triggerComponent: {
                name: 'dynamic-components/icon',
                options: { icon: 'question-circle', prefix: 'far', className: 'pikachu-icon' }
              },
              className: 'pikachu',
              triggerOn: 'hover',
              contentComponents: [
                {
                  name: 'dynamic-components/text',
                  options: { text: 'Click here to learn how pikachu became a stardew valley gamer!' }
                },
                {
                  name: 'dynamic-components/wiki-link',
                  options: { wikiKey: 'appHelp', className: 'pikachu-miner-link' }
                }
              ]
            }
          }
        ]
      }
    };
    this.set('props', sampleRenderProps);

    await render(hbs`{{component this.props.name options=this.props.options}}`);
    assert.dom('.dynamic-tooltip').exists('Renders a dynamic tooltip');
    assert.dom('.dynamic-header').exists('Renders a dynamic header');
    assert.dom('.mining').exists('Gives us the correct custom class');

    assert.dom('.pikachu-icon').exists('Renders the nested icon element');
    assert.dom('.dynamic-header__title').hasTextContaining('Gotta mine em all!');

    await triggerEvent('.dynamic-tooltip', 'mouseenter');
    assert.dom('.dynamic-tooltip__content .pikachu-miner-link').exists('Renders the nested link');
    assert.dom('.dynamic-header__content').hasTextContaining('Mining tutorial');
  });
});
