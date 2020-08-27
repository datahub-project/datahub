import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import {
  IDatasetsCorePagePlatformDiscriminatorArgs,
  Discriminator
} from '@datahub/datasets-core/components/datasets/core/page/platform-discriminator';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import Component from '@ember/component';

module('Integration | Component | datasets/core/page/platform-discriminator', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.owner.register('component:layout1', Component.extend({ layout: hbs`layout1` }));
    this.owner.register('component:layout2', Component.extend({ layout: hbs`layout2` }));
    this.owner.register('component:layout3', Component.extend({ layout: hbs`layout3` }));

    const discriminator: Discriminator = {
      [DatasetPlatform.HDFS]: {
        name: 'layout1',
        options: {}
      },
      [DatasetPlatform.Hive]: {
        name: 'layout2',
        options: {}
      }
    };
    const args: IDatasetsCorePagePlatformDiscriminatorArgs = {
      urn: 'urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)',
      options: {
        default: {
          name: 'layout3',
          options: {}
        },
        discriminator
      }
    };
    this.setProperties(args);

    await render(hbs`<Datasets::Core::Page::PlatformDiscriminator @urn={{urn}} @options={{options}}/>`);
    assert.dom().containsText('layout1');
    assert.dom().doesNotContainText('layout2');
    assert.dom().doesNotContainText('layout3');

    this.set('urn', 'urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)');
    assert.dom().containsText('layout2');
    assert.dom().doesNotContainText('layout1');
    assert.dom().doesNotContainText('layout3');

    this.set('urn', 'urn:li:dataset:(urn:li:dataPlatform:other,test,PROD)');
    assert.dom().containsText('layout3');
    assert.dom().doesNotContainText('layout1');
    assert.dom().doesNotContainText('layout2');
  });
});
