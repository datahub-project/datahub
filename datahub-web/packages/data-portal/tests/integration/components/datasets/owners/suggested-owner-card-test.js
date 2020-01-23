import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll, find, triggerEvent } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

import owners from 'wherehows-web/mirage/fixtures/owners';
import { OwnerType } from 'wherehows-web/utils/api/datasets/owners';
import { noop } from 'wherehows-web/utils/helpers/functions';

module('Integration | Component | datasets/owners/suggested owner card', function(hooks) {
  setupRenderingTest(hooks);

  const [confirmedOwner, suggestedOwner] = owners;
  const ownerTypes = Object.values(OwnerType);
  const commonOwners = [confirmedOwner];

  const fullNameClass = '.suggested-owner-card__owner-info__profile__name__full';
  const usernameClass = '.suggested-owner-card__owner-info__profile__name__username';
  const addedClass = '.suggested-owner-card__owner-info__add--disabled';
  const addButtonClass = '.nacho-button--secondary.nacho-button--medium';
  const sourceClass = '.suggested-owner-card__source-info';

  test('it renders for base and empty cases', async function(assert) {
    this.setProperties({
      commonOwners,
      author: { owner: {} },
      ownerTypes: [],
      removeOwner: noop,
      confirmSuggestedOwner: noop
    });

    await render(hbs`{{datasets/owners/suggested-owner-card
                      owner=author
                      ownerTypes=ownerTypes
                      commonOwners=commonOwners
                      removeOwner=removeOwner
                      confirmSuggestedOwner=confirmSuggestedOwner}}`);

    assert.ok(this.element, 'Renders independently without errors');
    assert.ok(this.element, 'Empty owner does not create breaking error');
  });

  test('it renders the correct information for suggested owner', async function(assert) {
    const model = suggestedOwner;
    const fullNameText = model.name;
    const usernameText = model.userName;
    const sourceText = `Source: ${model.source}`;

    this.setProperties({
      ownerTypes,
      commonOwners,
      author: { owner: model },
      removeOwner: noop,
      confirmSuggestedOwner: noop
    });

    await render(hbs`{{datasets/owners/suggested-owner-card
                      owner=author
                      ownerTypes=ownerTypes
                      commonOwners=commonOwners
                      removeOwner=removeOwner
                      confirmSuggestedOwner=confirmSuggestedOwner}}`);

    assert.ok(this.element, 'Still renders without errors');

    assert.equal(find(fullNameClass).textContent.trim(), fullNameText, 'Renders the name correctly');

    assert.equal(find(usernameClass).textContent.trim(), usernameText, 'Renders the username correctly');

    assert.equal(find(sourceClass).textContent.trim(), sourceText, 'Renders the source correctly');

    assert.equal(findAll(addedClass).length, 0, 'Does not consider suggested owner already added');
    assert.equal(findAll(addButtonClass).length, 1, 'Renders add button for suggested class');
  });

  test('it functions correctly to add a suggested owner', async function(assert) {
    const model = suggestedOwner;

    this.setProperties({
      ownerTypes,
      commonOwners,
      author: { owner: model },
      removeOwner: noop,
      confirmSuggestedOwner: owner => {
        assert.equal(owner.name, model.name, 'Passes the correct information to the confirmOwner function');
      }
    });

    await render(hbs`{{datasets/owners/suggested-owner-card
                      owner=author
                      ownerTypes=ownerTypes
                      commonOwners=commonOwners
                      removeOwner=removeOwner
                      confirmSuggestedOwner=confirmSuggestedOwner}}`);

    assert.ok(this.element, 'Still renders without errors for real function passed in');
    triggerEvent(addButtonClass, 'click');
  });
});
