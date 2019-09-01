import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, triggerEvent, findAll, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { nonHdfsUrn } from 'wherehows-web/mirage/fixtures/urn';
import fabrics from 'wherehows-web/mirage/fixtures/fabrics';

module('Integration | Component | datasets/dataset-fabric-switcher', function(hooks) {
  setupRenderingTest(hooks);

  const contentSelector = '.dataset-fabric-switcher__fabrics';
  const triggerSelector = '.dataset-fabric-switcher__fabric';

  test('component rendering', async function(assert) {
    assert.expect(1);

    await render(hbs`
      {{datasets/dataset-fabric-switcher}}
    `);

    assert.ok(this.element.querySelector('.dataset-fabric-switcher'), 'expected component class exists in DOM');
  });

  test('toggle action shows and hides dropdown', async function(assert) {
    assert.expect(3);

    this.set('urn', nonHdfsUrn);

    await render(hbs`
      {{datasets/dataset-fabric-switcher urn=urn}}
    `);

    assert.notOk(find(contentSelector), 'expected dropdown content class is hidden');

    await triggerEvent(triggerSelector, 'mouseenter');

    assert.ok(find(contentSelector), 'expected dropdown content class is shown');

    await triggerEvent(triggerSelector, 'mouseleave');

    assert.notOk(find(contentSelector), 'expected dropdown content class is hidden');
  });

  test('rendering of fabrics', async function(assert) {
    assert.expect(1);

    this.set('urn', nonHdfsUrn);
    this.setProperties({
      urn: nonHdfsUrn,
      fabrics
    });

    await render(hbs`
      {{datasets/dataset-fabric-switcher urn=urn fabrics=fabrics}}
    `);

    await triggerEvent(triggerSelector, 'mouseenter');

    assert.equal(
      findAll(`${contentSelector} li`).length,
      fabrics.length,
      'expected fabrics shown to equal default list of fabrics'
    );
  });
});
