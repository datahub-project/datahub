import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, settled } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { getTextNoSpaces } from '@datahub/utils/test-helpers/dom-helpers';
import { setProperties } from '@ember/object';
import { TestContext } from 'ember-test-helpers';

interface INachoTabCacherTest {
  lazyRender: boolean;
  id: string;
  currentTab: string;
  content: unknown;
}

type IMyTestContext = INachoTabCacherTest & TestContext;

const initTest = async (
  test: IMyTestContext,
  overrides: Partial<INachoTabCacherTest>
): Promise<{ getNCalls: () => number }> => {
  let getterCalls = 0;
  setProperties(test, {
    lazyRender: false,
    id: 'one',
    currentTab: 'two',
    content: {
      get text(): string {
        getterCalls += 1;
        return 'somecontenthere';
      }
    },
    ...overrides
  });

  await render(hbs`
    {{#nacho/nacho-tab-cacher
      lazyRender=lazyRender
      id=id
      currentTab=currentTab
    }}
      {{content.text}}
    {{/nacho/nacho-tab-cacher}}
  `);

  return {
    getNCalls(): number {
      return getterCalls;
    }
  };
};

module('Integration | Component | nacho/nacho-tab-cacher', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders lazy render off', async function(this: IMyTestContext, assert) {
    const myTestApi = await initTest(this, { lazyRender: false });
    assert.equal(getTextNoSpaces(this), 'somecontenthere', 'No lazy render should render initially');

    setProperties(this, {
      currentTab: 'one'
    });

    await settled();
    assert.equal(getTextNoSpaces(this), 'somecontenthere', 'No lazy render should always show');

    setProperties(this, {
      currentTab: 'two'
    });

    await settled();
    assert.equal(getTextNoSpaces(this), 'somecontenthere', 'No lazy render should always show');
    assert.equal(myTestApi.getNCalls(), 1, 'No rerenders');
  });

  test('it renders lazy render on', async function(this: IMyTestContext, assert) {
    const myTestApi = await initTest(this, { lazyRender: true });
    assert.equal(getTextNoSpaces(this), '', 'Lazy render should not render initially');

    setProperties(this, {
      currentTab: 'one'
    });

    await settled();
    assert.equal(getTextNoSpaces(this), 'somecontenthere', 'Lazy render should render');

    setProperties(this, {
      currentTab: 'two'
    });

    await settled();
    assert.equal(getTextNoSpaces(this), 'somecontenthere', 'Lazy render should cache');
    assert.equal(myTestApi.getNCalls(), 1, 'Getter happens only once');
  });
});
