import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | get-entity-by-urn', function(hooks) {
  setupRenderingTest(hooks);

  test('it returns the correct type of entity', async function(assert) {
    this.set('urn', 'urn:li:dataset:(urn:li:dataPlatform:hdfs,pikachu,PROD)');
    await render(hbs`
      <GetEntityByUrn @urn={{urn}} as |dataset|>
        <span id="assert-test-1">{{dataset.displayName}}</span>
      </GetEntityByUrn>
    `);

    assert.dom('#assert-test-1').hasText('datasets', 'Correctly yielded dataset entity');

    this.set('urn', 'urn:li:corpuser:pikachu');
    await render(hbs`
      <GetEntityByUrn @urn={{urn}} as |person|>
        <span id="assert-test-2">{{person.displayName}}</span>
      </GetEntityByUrn>
    `);

    assert.dom('#assert-test-2').hasText('people', 'Correctly yielded person entity');
  });
});
