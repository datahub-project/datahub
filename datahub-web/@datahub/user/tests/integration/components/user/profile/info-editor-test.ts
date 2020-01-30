import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { baseInfoEditorClass } from '@datahub/user/components/user/profile/entity-header';
import { populateMockPersonEntity } from '@datahub/user/mocks/person-entity';
import { IPersonEntityEditableProperties } from '@datahub/data-models/types/entity/person/props';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import UserProfileInfoEditor from '@datahub/user/components/user/profile/info-editor';
import { set } from '@ember/object';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

module('Integration | Component | user/profile/info-editor', function(hooks): void {
  setupRenderingTest(hooks);

  const baseClass = `.${baseInfoEditorClass}`;
  const editorItem = `${baseClass}__item`;

  test('it renders and behaves as expected', async function(assert): Promise<void> {
    await render(hbs`<User::Profile::InfoEditor />`);
    assert.ok(this.element, 'Initial render is without errors');
    assert.ok(find(baseClass), 'Renders an element of the expected class');
  });

  test('it behaves as expected', async function(assert): Promise<void> {
    assert.expect(8);

    this.setProperties({
      entity: populateMockPersonEntity(new PersonEntity('pikachu')),
      onSave(value: IPersonEntityEditableProperties): void {
        assert.ok(value, 'A value was given to the save action');
        assert.equal(value.skills.length, 4, 'Edited skills were passed to the action');
      }
    });

    const component = await getRenderedComponent({
      ComponentToRender: UserProfileInfoEditor,
      componentName: 'user/profile/info-editor',
      testContext: this,
      template: hbs`<User::Profile::InfoEditor @entity={{entity}} @onSave={{onSave}} />`
    });

    assert.equal(component.editedProps.skills.length, 3, 'Renders expected default values for edited');
    assert.equal(
      (find(`${editorItem}:first-child ${baseClass}__text-input`)! as HTMLTextAreaElement).value,
      'Trying to catch all these Pokemon in the world. Also being the very best like no one ever was.',
      "Renders text from the person entity context's focus area"
    );

    set(component, 'skills', component.skills.concat('leveling'));
    assert.equal(component.skills.length, 4, 'Setting new skills adjusts number of skills shown');
    assert.equal(component.skills[3], 'leveling', 'Last tag added is in the skills');
    assert.equal(component.editedProps.skills.length, 4, 'Edited props was changed when updating skills');

    await click(`${baseClass}__action:nth-child(2)`);

    assert.equal(component.editedProps.skills.length, 0, 'After saving returns items to default');
  });
});
