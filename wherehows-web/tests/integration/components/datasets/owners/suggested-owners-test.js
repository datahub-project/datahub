import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('datasets/owners/suggested-owners', 'Integration | Component | datasets/owners/suggested owners', {
  integration: true
});

const descriptionClass = '.dataset-authors-suggested__info__description';
const suggestedCardClass = '.suggested-owner-card';

test('it renders properly for null case and empty states', function(assert) {
  const descriptionText = 'We found no suggested owner(s) for this dataset, based on scanning different systems';

  this.render(hbs`{{datasets/owners/suggested-owners}}`);

  assert.ok(this.$(), 'Renders without errors when passed no values');
  assert.equal(this.$(suggestedCardClass).length, 0, 'Renders no cards');

  assert.equal(
    this.$(descriptionClass)
      .text()
      .trim(),
    descriptionText,
    'Renders the correct message when there are no owners'
  );
});
