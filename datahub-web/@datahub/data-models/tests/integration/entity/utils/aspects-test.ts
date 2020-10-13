import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, settled } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';
import { aspect, hasAspect, setAspect } from '@datahub/data-models/entity/utils/aspects';

declare module '@datahub/data-models/entity/utils/aspects' {
  export interface IAvailableAspects {
    ['aspect1']?: string;
  }
}

module('Integration | Aspects | aspects', function(hooks) {
  setupRenderingTest(hooks);

  test('Tests a real use case of an aspect lifecycle', async function(assert) {
    class AspectedMockEntity extends MockEntity {
      entity: {
        urn: string;
        removed: boolean;
        aspect1: string;
      } = {
        urn: 'urn:li:dataset:(urn:li:dataPlatform:hdfs,pikachu,PROD)',
        removed: false,
        aspect1: 'valueaspect1'
      };
      @aspect('someaspectname')
      aspect1!: string;
    }
    const mockEntity = new AspectedMockEntity();

    this.set('entity', mockEntity);
    await render(hbs`{{this.entity.aspect1}}`);

    assert.dom().hasText('valueaspect1', 'Correctly reads aspect from entity');

    setAspect(mockEntity, 'aspect1', 'somethingelse');
    await settled();

    assert.dom().hasText('somethingelse', 'Correctly reads aspect from entity');

    assert.ok(hasAspect(mockEntity, 'aspect1'), 'it should have the aspect');
  });
});
