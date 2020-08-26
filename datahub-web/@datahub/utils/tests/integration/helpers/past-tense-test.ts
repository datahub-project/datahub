import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { pastTense } from 'dummy/helpers/past-tense';

module('Integration | Helper | past-tense', function(hooks) {
  setupRenderingTest(hooks);

  const tests: Record<string, string> = {
    bake: 'baked',
    smile: 'smiled',
    free: 'freed',
    dye: 'dyed',
    tiptoe: 'tiptoed',
    travel: 'traveled',
    model: 'modeled',
    distil: 'distilled',
    equal: 'equalled',
    admit: 'admitted',
    commit: 'committed',
    refer: 'referred',
    inherit: 'inherited',
    visit: 'visited',
    stop: 'stopped',
    tap: 'tapped',
    sob: 'sobbed',
    treat: 'treated',
    wheel: 'wheeled',
    pour: 'poured',
    picnic: 'picnicked',
    mimic: 'mimicked',
    traffic: 'trafficked'
  };

  test('it renders the correct value', function(assert) {
    for (const verb in tests) {
      assert.equal(pastTense([verb]), tests[verb], `Expected ${verb} to be ${tests[verb]}`);
    }
  });
});
