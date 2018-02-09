import { moduleFor, test } from 'ember-qunit';

moduleFor('route:datasets/dataset/compliance', 'Unit | Route | datasets/dataset/compliance', {
  needs: ['service:metrics']
});

test('it exists', function(assert) {
  let route = this.subject();
  assert.ok(route);
});
