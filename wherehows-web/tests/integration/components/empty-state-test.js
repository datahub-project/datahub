import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('empty-state', 'Integration | Component | empty state', {
  integration: true
});

test('it renders', function(assert) {
  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{empty-state}}`);

  assert.equal(
    this.$()
      .text()
      .trim(),
    'No data found'
  );

  // Template block usage:
  this.render(hbs`
    {{#empty-state}}
      template block text
    {{/empty-state}}
  `);

  assert.equal(
    this.$()
      .text()
      .trim(),
    'template block text'
  );
});

test('it renders a heading', function(assert) {
  const heading = 'Not found!';
  assert.expect(1);

  this.set('heading', heading);

  this.render(hbs`{{empty-state heading=heading}}`);

  assert.equal(
    this.$()
      .text()
      .trim(),
    heading,
    'shows the heading text'
  );
});

test('it renders a subheading', function(assert) {
  const subHeading = 'We could not find any results.';
  assert.expect(1);

  this.set('subHeading', subHeading);

  this.render(hbs`{{empty-state subHead=subHeading}}`);

  assert.equal(
    this.$('.empty-state__sub-head')
      .text()
      .trim(),
    subHeading,
    'shows the subheading text'
  );
});
