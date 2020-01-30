import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, findAll, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { baseHeaderClass, baseInfoEditorClass } from '@datahub/user/components/user/profile/entity-header';
import { populateMockPersonEntity } from '@datahub/user/mocks/person-entity';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { IPersonEntityEditableProperties } from '@datahub/data-models/types/entity/person/props';
import { getTextNoSpacesFromElement } from '@datahub/utils/test-helpers/dom-helpers';

module('Integration | Component | user/profile/entity-header', function(hooks): void {
  setupRenderingTest(hooks);

  const baseClass = `.${baseHeaderClass}`;
  const baseEditorClass = `.${baseInfoEditorClass}`;
  const editProfileButtonClass = `${baseClass}__edit-profile`;

  test('it renders', async function(assert): Promise<void> {
    await render(hbs`<User::Profile::EntityHeader />`);
    assert.ok(this.element, 'Initial render is without errors');
    assert.ok(find(baseClass), 'Renders the expected element');
  });

  test('it behaves as expected', async function(assert): Promise<void> {
    assert.expect(8);

    this.setProperties({
      entity: populateMockPersonEntity(new PersonEntity('pikachu')),
      headerProperties: PersonEntity.allRenderProps.userProfilePage.headerProperties,
      entityPageProps: PersonEntity.allRenderProps.entityPage,
      onSave(values: IPersonEntityEditableProperties): void {
        assert.ok(values, 'Action was handled correctly and values were passed to action');
        assert.equal(
          values.focusArea,
          'Trying to catch all these Pokemon in the world. Also being the very best like no one ever was.',
          'Gives a correct value to the save function'
        );
      }
    });

    await render(hbs`<User::Profile::EntityHeader
                       @entity={{entity}}
                       @headerProperties={{headerProperties}}
                       @entityPageProps={{entityPageProps}}
                       @onSave={{onSave}}
                       @isCurrentUser={{true}}
                     />`);

    assert.ok(this.element, 'Initial render is still without errors');
    assert.equal(find(`${baseClass}__name`)!.textContent!.trim(), 'Ash Ketchum', 'Renders expected name');

    assert.equal(
      getTextNoSpacesFromElement(find(`${baseClass}__manager`)!),
      'MANAGERPikachu',
      'Renders manager as expected'
    );

    assert.equal(
      getTextNoSpacesFromElement(find(`${baseClass}__team-tags`)!),
      'KantoNintendoGameFreakBikeThief',
      'Renders team tags as expected'
    );

    assert.equal(findAll(baseEditorClass).length, 0, 'By default does not render modal');

    await click(editProfileButtonClass);

    assert.equal(findAll(baseEditorClass).length, 1, 'Shows modal once we have profile editing');

    await click(`${baseEditorClass}__action:nth-child(2)`);
  });
});
