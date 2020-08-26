import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { populateMockPersonEntity } from '@datahub/user/mocks/person-entity';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

module('Integration | Component | user/containers/entity-header', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    await render(hbs`<User::Containers::EntityHeader />`);
    assert.ok(this.element, 'Initial render is without errors');
  });

  test('is functions as expected', async function(assert): Promise<void> {
    const entity = populateMockPersonEntity(new PersonEntity('pikachu'));
    let taskHasRun = false;

    entity.updateEditableProperties = () =>
      new Promise(() => {
        taskHasRun = true;
      });

    this.set('entity', entity);

    await render(hbs`<User::Containers::EntityHeader @entity={{entity}} as |data|>
                       <button class="test-button" onclick={{perform data.updateProfileTask}} type="button">
                         Click ME!
                       </button>
                       <span class="test-span">{{data.entity.name}}</span>
                     </User::Containers::EntityHeader>`);

    assert.ok(this.element, 'Element still renders without errors given information');
    assert.equal(
      find('.test-span')!.textContent!.trim(),
      'Ash Ketchum',
      'Container gives access to entity as expected'
    );

    await click('.test-button');

    assert.ok(taskHasRun, 'Task runs when perform is triggered as expected');
  });
});
