import initializerExport from '@datahub/tracking/initializers/unified-tracking';
import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

// Asserts the name property on the default export object from initializers
const defaultExport = initializerExport as { initialize: (...args: Array<any>) => void; name: string };

module('Unit | Initializer | unified-tracking', function(hooks): void {
  setupTest(hooks);

  // This does not test any specific functionality
  // this test just ensures that an initializer exists that is called unified-tracking so it can be referenced by name
  test('it exists and is not renamed', function(assert): void {
    assert.ok(
      typeof defaultExport.initialize === 'function',
      'Expected the initializer to exist at the referenced location for UnifiedTracking'
    );

    assert.equal(
      defaultExport.name,
      'unified-tracking',
      'Expected initializer default to not have a name property export'
    );
  });
});
