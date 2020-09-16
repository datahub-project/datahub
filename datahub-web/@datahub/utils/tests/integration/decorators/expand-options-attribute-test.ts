import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import Component from '@ember/component';
import { expandOptionsAttribute } from '@datahub/utils/decorators/expand-options-attribute';
import { layout } from '@ember-decorators/component';

@layout(hbs`{{text}}`)
@expandOptionsAttribute()
@expandOptionsAttribute('params')
class ComponentA extends Component {
  text?: string;
}

module('Integration | Decorator | expandOptionsAttribute', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders the options bag correctly', async function(assert): Promise<void> {
    this.owner.register('component:component-a', ComponentA);

    await render(hbs`<ComponentA @options={{hash text="Hola"}} />`);

    assert.equal(this.element.textContent && this.element.textContent.trim(), 'Hola');

    await render(hbs`<ComponentA @params={{hash text="Adios"}} />`);

    assert.equal(this.element.textContent && this.element.textContent.trim(), 'Adios');
  });
});
