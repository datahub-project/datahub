import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | dynamic-components/icon', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders and behaves as expected', async function(assert) {
    const sampleRenderProps = {
      name: 'dynamic-components/icon',
      options: { icon: 'question-circle', prefix: 'far', className: 'pikachu-icon' }
    };

    this.set('props', sampleRenderProps);

    await render(hbs`{{component this.props.name options=this.props.options}}`);
    assert.dom('.dynamic-fa-icon').exists('Renders our icon as expected');
    assert.dom('.pikachu-icon').exists('Renderse with our custom class name');
    assert.dom('svg[data-icon="question-circle"]').exists('Renders an icon according to our prop');
    assert.dom('svg[data-prefix="far"]').exists('Renders an icon with our desired prefix');
  });
});
