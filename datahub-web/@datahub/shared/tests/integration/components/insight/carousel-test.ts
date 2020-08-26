import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';
import { IInsightCarouselCardProps } from '@datahub/shared/types/insight/carousel/card';
import Component from '@ember/component';
import { layout } from '@ember-decorators/component';

module('Integration | Component | insight/carousel', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    @layout(hbs`{{@options.priority}}`)
    class DummyComponent extends Component {}

    this.owner.register('component:dummy-component', DummyComponent);

    const components: Array<IInsightCarouselCardProps> = [
      {
        name: 'dummy-component',
        options: {
          priority: 2
        }
      },
      {
        name: 'dummy-component',
        options: {
          priority: 1
        }
      },
      {
        name: 'dummy-component',
        options: {
          priority: undefined
        }
      }
    ];
    const entity = new MockEntity('test-urn');
    const options = {
      components
    };

    await render(hbs`<Insight::Carousel @entity={{this.entity}} @options={{this.options}} />`);
    assert.dom().hasNoText();
    assert.dom('.insight-carousel').exists();

    this.setProperties({ entity, options });

    assert.dom().hasText('1 2');
  });
});
