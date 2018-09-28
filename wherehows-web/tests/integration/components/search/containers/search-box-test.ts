import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import { getText } from 'wherehows-web/tests/helpers/dom-helpers';
import hbs from 'htmlbars-inline-precompile';
import Component from '@ember/component';
import { TaskInstance } from 'ember-concurrency';
import { TestContext } from 'ember-test-helpers';

interface ITestWithMirageContext extends TestContext {
  server: any;
}

const getMirageHandle = (test: ITestWithMirageContext, api: string, verb: string) => {
  return test.server.pretender.hosts.forURL(api)[verb.toLocaleUpperCase()].recognize(api)[0].handler;
};

const containerComponentTest = (test: TestContext, testFn: (me: Component) => void) => {
  test.owner.register(
    'component:container-stub',
    class ContainerStub extends Component {
      didInsertElement() {
        testFn(this);
      }
    }
  );
};

interface IContainerStub extends Component {
  onUserType: (word: string) => TaskInstance<Array<string>>;
}

module('Integration | Component | search/containers/search-box', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`
      {{#search/containers/search-box  as |keyword placeholder onUserType onSearch|}}
        template block text
      {{/search/containers/search-box}}
    `);

    assert.equal(getText(this).trim(), 'template block text');
  });

  test('onUserType', async function(this: ITestWithMirageContext, assert) {
    const apiHandler = getMirageHandle(this, '/api/v1/autocomplete/datasets', 'get');
    assert.expect(6);

    containerComponentTest(this, async (component: IContainerStub) => {
      // dont return anything with less than 3
      const results1 = await component.onUserType('h');
      assert.equal(results1.length, 0);

      // return list
      const results2 = await component.onUserType('hol');
      assert.ok(results2.length > 0);

      // cache return
      const results3 = await component.onUserType('hol');
      assert.ok(results3.length > 0);
      assert.equal(apiHandler.numberOfCalls, 1, 'cached return');

      // debounce
      component.onUserType('hola');
      component.onUserType('hola ');
      const results4 = await component.onUserType('hola nacho');
      assert.ok(results4.length > 0);
      assert.equal(apiHandler.numberOfCalls, 2, 'App debounces calls');
    });

    await render(hbs`
      {{#search/containers/search-box  as |keyword placeholder onUserType onSearch|}}
        {{container-stub onUserType=(action onUserType)}}
      {{/search/containers/search-box}}
    `);
  });
});
