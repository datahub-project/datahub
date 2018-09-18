import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { getText } from 'wherehows-web/tests/helpers/dom-helpers';

module('Integration | Component | get-yield', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders and displays correct value', async function(assert) {
    const myObject = {
      hello: 'world'
    };

    this.setProperties({
      object: myObject,
      property: 'hello'
    });

    await render(hbs`
      {{#get-yield object=object property=property as |text|}}
        {{text}}
      {{/get-yield}}
    `);

    assert.equal(getText(this), 'world');
  });

  test('it renders and displays default value', async function(assert) {
    const myObject = {};

    this.setProperties({
      object: myObject,
      property: 'hello'
    });

    await render(hbs`
      {{#get-yield object=object property=property default='world' as |text|}}
        {{text}}
      {{/get-yield}}
    `);

    assert.equal(getText(this), 'world');
  });
});
