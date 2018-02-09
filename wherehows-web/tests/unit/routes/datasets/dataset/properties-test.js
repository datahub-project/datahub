import { moduleFor, test } from 'ember-qunit';

moduleFor('route:datasets/dataset/properties', 'Unit | Route | datasets/dataset/properties', {
  needs: ['service:metrics']
});

test('it exists', function(assert) {
  let route = this.subject();
  assert.ok(route);
});
