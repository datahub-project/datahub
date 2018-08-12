import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | datasets/owners/suggested owners', function(hooks) {
  setupRenderingTest(hooks);

  const descriptionClass = '.dataset-authors-suggested__info__description';
  const suggestedCardClass = '.suggested-owner-card';

  test('it renders properly for null case and empty states', async function(assert) {
    const descriptionText = 'We found no suggested owner(s) for this dataset, based on scanning different systems';

    await render(hbs`{{datasets/owners/suggested-owners}}`);

    assert.ok(this.element, 'Renders without errors when passed no values');
    assert.equal(findAll(suggestedCardClass).length, 0, 'Renders no cards');

    assert.equal(
      find(descriptionClass).textContent.trim(),
      descriptionText,
      'Renders the correct message when there are no owners'
    );
  });
});
