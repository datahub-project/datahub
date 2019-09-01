import { module, skip } from 'qunit';
import { visit, currentURL, findAll, click } from '@ember/test-helpers';
import { setupApplicationTest } from 'ember-qunit';
import { IMirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import defaultScenario from 'wherehows-web/mirage/scenarios/default';
import appLogin from 'wherehows-web/tests/helpers/login/test-login';
// import { supportedListEntities } from '@datahub/lists/constants/entity/shared';
import { authenticationUrl } from 'wherehows-web/tests/helpers/login/constants';
import { IStoredEntityAttrs } from '@datahub/lists/types/list';
import { stubService } from '@datahub/utils/test-helpers/stub-service';

const entityPageLayoutClass = '.entity-list';
const listCountComponentClass = '.entity-list-count';
const storage: Array<IStoredEntityAttrs> = [];
// const [aListEntity] = supportedListEntities;
const entityDisplayName = 'someentity'; //aListEntity.displayName
const listRoute = `/lists/${entityDisplayName}`;

module('Acceptance | entity lists', function(hooks) {
  setupApplicationTest(hooks);

  hooks.beforeEach(function() {
    // Stub the list manager service and add entities
    stubService(
      'entity-lists-manager',
      (() => {
        storage.setObjects([]);

        return {
          addToList(instance: IStoredEntityAttrs) {
            storage.addObject(instance);
          },
          removeFromList(entities: Array<IStoredEntityAttrs>) {
            const objects = [].concat.apply(entities).map(({ urn }: IStoredEntityAttrs) => storage.findBy('urn', urn));
            objects && storage.removeObjects(objects as Array<IStoredEntityAttrs>);
          },
          get entities() {
            return {
              [entityDisplayName]: storage
            };
          }
        };
      })()
    );
  });

  skip('visiting /lists/:entity route', async function(this: IMirageTestContext, assert) {
    assert.expect(9);

    // Attempting accessing lists before authentication, should redirect to login
    await visit(listRoute);
    assert.equal(
      currentURL(),
      authenticationUrl,
      `Route for ${entityDisplayName} is NOT navigable before user is authenticated`
    );

    await appLogin();
    await visit(listRoute);

    assert.equal(currentURL(), listRoute, `Route for ${entityDisplayName} list exists and is navigable after login`);
    assert.dom(entityPageLayoutClass).exists();
    assert.dom(entityPageLayoutClass).hasClass('nacho-container');
    assert.dom('.empty-state').exists(`Expected no items to be in the ${entityDisplayName} list`);

    assert.dom(listCountComponentClass).exists('Expected list count component to be rendered in DOM');
    assert
      .dom(listCountComponentClass)
      .isNotVisible('Expected list count component to be hidden when the list is empty');
    assert
      .dom(listCountComponentClass)
      .containsText('List (0)', 'Expected the list count component to have the name List and a count value of 0');
    assert
      .dom(`.navbar ${listCountComponentClass}`)
      .exists('Expected the list count component to be embedded within the main navigation bar');
  });

  skip('adding items to a list and visiting /lists/:entity route', async function(this: IMirageTestContext, assert) {
    assert.expect(16);

    // Add two serialized entities to storage list
    storage.addObjects([
      { urn: 'urn:li:feature:(0)', type: 'features' },
      { urn: 'urn:li:feature:(1)', type: 'features' }
    ]);

    const storageCount = storage.length;
    const urns: Array<string> = storage.mapBy('urn');

    defaultScenario(this.server);

    await appLogin();
    await visit(listRoute);

    assert.dom(listCountComponentClass).isVisible();
    assert
      .dom(listCountComponentClass)
      .containsText(
        `List (${storageCount})`,
        `Expected the list count component to have the name List and a count value of ${storageCount}`
      );

    assert.dom(`${entityPageLayoutClass}__page-meta`).containsText(`Showing ${storageCount} of ${storageCount}`);
    assert.dom(`${entityPageLayoutClass}__title`).containsText(`list`);

    const groupCheckBox = '#group-checkbox-for-feature';
    const checkboxClasses = `${entityPageLayoutClass}__checkbox`;
    const items = findAll(`${entityPageLayoutClass}__item`);
    const checkboxes = findAll(checkboxClasses);
    assert.equal(items.length, storageCount, `Expected to render ${storageCount} entities`);

    assert.equal(checkboxes.length, storageCount + 1, `Expected to find a global checkbox and one for each entity`);

    assert.dom(groupCheckBox).isNotChecked();
    urns
      .map(urn => `#${urn.replace(/[:()]/g, char => `\\${char}`)}-checkbox`)
      .forEach(checkbox => assert.dom(checkbox).isNotChecked());
    // Check group checkbox
    await click(groupCheckBox);
    assert.dom(groupCheckBox).isChecked();
    urns
      .map(urn => `#${urn.replace(/[:()]/g, char => `\\${char}`)}-checkbox`)
      .forEach(checkbox => assert.dom(checkbox).isChecked());

    assert
      .dom(`${entityPageLayoutClass}__selection-meta`)
      .hasText(
        `2 ${entityDisplayName} selected`,
        'Expected the selection metadata text field to show the number of entities selected'
      );

    assert.dom(`${entityPageLayoutClass}__page-meta .entity-list-toggle`).exists();
    assert
      .dom(`${entityPageLayoutClass}__page-meta`)
      .containsText(
        'Remove from list Get Config',
        'Expected a config button to be in the entity page metadata actions'
      );

    // Remove entities by clicking the toggle button which is the group toggle
    await click('.entity-list-toggle');

    assert.equal(
      findAll(`${entityPageLayoutClass}__item`).length,
      0,
      ' Expected all entities to be removed from the list of entities'
    );
  });

  skip('adding items to a list and visiting /lists/:entity route', async function(this: IMirageTestContext, assert) {
    assert.expect(1);
    // Add a serialized entities to storage list
    storage.addObject({ urn: 'urn:li:feature:(0)', type: 'features' });

    defaultScenario(this.server);

    await appLogin();
    await visit('/');

    await click(`${listCountComponentClass} a`);

    assert.equal(
      currentURL(),
      listRoute,
      `Navigates to list route for ${entityDisplayName} when the list count component is clicked`
    );
  });
});
