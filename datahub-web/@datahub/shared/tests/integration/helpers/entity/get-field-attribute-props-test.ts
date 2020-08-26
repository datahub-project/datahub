import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';

module('Integration | Helper | entity/get-field-attribute-props', function(hooks) {
  setupRenderingTest(hooks);

  test('it gives the expected attribute', async function(assert) {
    const sampleEntity = new MockEntity('urn:li:mock:pikachu');
    this.set('entity', sampleEntity);
    await render(hbs`
      <div id="my-test">
        {{get (entity/get-field-attribute-props entity "pikachu") "displayName"}}
      </div>
    `);
    assert.dom('#my-test').containsText('Pikachu', 'Renders the expected object');

    await render(hbs`
      <div id="my-test">
        {{get (entity/get-field-attribute-props entity "badvalue") "displayName"}}
      </div>
    `);

    assert.dom('#my-test').containsText('', 'Bad values give no value');
  });
});
