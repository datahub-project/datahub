import { moduleFor, test } from 'ember-qunit';

moduleFor('route:datasets/dataset/relations', 'Unit | Route | datasets/dataset/relations', {
  needs: ['service:metrics']
});

test('it exists', function(assert) {
  let route = this.subject();
  assert.ok(route);
});
