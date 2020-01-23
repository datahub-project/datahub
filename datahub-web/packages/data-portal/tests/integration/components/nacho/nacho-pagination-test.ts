import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { getTextNoSpaces } from '@datahub/utils/test-helpers/dom-helpers';
import { setProperties } from '@ember/object';
import { TestContext } from 'ember-test-helpers';

interface INachoPaginagionTestContext extends TestContext {
  currentPage: number;
  totalPages: number;
}

const myTests = [
  {
    expected: 'Previous1234...6Next',
    params: {
      currentPage: 1,
      totalPages: 6
    }
  },
  {
    expected: 'Previous1Next',
    params: {
      currentPage: 1,
      totalPages: 1
    }
  },
  {
    expected: 'Previous12Next',
    params: {
      currentPage: 1,
      totalPages: 2
    }
  },
  {
    expected: 'Previous123Next',
    params: {
      currentPage: 1,
      totalPages: 3
    }
  },
  {
    expected: 'Previous1234Next',
    params: {
      currentPage: 1,
      totalPages: 4
    }
  },
  {
    expected: 'Previous1...78910111213...20Next',
    params: {
      currentPage: 10,
      totalPages: 20
    }
  }
];

module('Integration | Component | nacho/nacho-pagination', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(this: INachoPaginagionTestContext, assert) {
    await myTests.reduce(async (promise, myTest) => {
      await promise;
      setProperties(this, myTest.params);
      await render(hbs`
        {{nacho/nacho-pagination
          currentPage=currentPage
          totalPages=totalPages
          linkTo=(hash route="someroute")
        }}
      `);
      assert.equal(getTextNoSpaces(this), myTest.expected);
    }, {});
  });
});
