import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, waitFor, waitUntil } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { TestContext } from 'ember-test-helpers';
import { noop } from 'lodash';

interface IParams {
  nItems: number;
  chunkSize?: number;
  onFinished?: (currentIndex?: number) => void;
}

const testNItems = 100;
const renderList = async (test: TestContext, params: IParams): Promise<void> => {
  const { nItems, chunkSize = 1, onFinished = noop } = params;
  const items: Array<number> = Array.from({ length: nItems }, (_: undefined, i: number): number => i);

  test.setProperties({
    items,
    chunkSize,
    onFinished
  });

  await render(hbs`
    <div>
      <BigList @list={{items}} @chunkSize={{chunkSize}} @onFinished={{onFinished}} as |item|>
        <span class="number">{{item}}</span>
      </BigList>
    </div>
  `);

  await waitFor('.number', { timeout: 10000, count: nItems });
};

const assertNumbers = (nItems: number, assert: Assert): void => {
  for (let i = 0; i < nItems; i++) {
    assert.dom(`.number:nth-child(${i + 1})`).hasText(`${i}`);
  }
};

module('Integration | Component | big-list', function(hooks): void {
  setupRenderingTest(hooks);

  test('Base use case', async function(assert): Promise<void> {
    await renderList(this, { nItems: testNItems });
    assertNumbers(testNItems, assert);
  });

  test('Chunk size', async function(assert): Promise<void> {
    await renderList(this, { nItems: testNItems, chunkSize: 7 });
    assertNumbers(testNItems, assert);
  });

  test('OnFinished action', async function(assert): Promise<void> {
    assert.expect(1);
    let finished = false;
    await renderList(this, {
      nItems: testNItems,
      chunkSize: 10,
      onFinished: () => {
        assert.ok(true);
        finished = true;
      }
    });
    await waitUntil(() => finished, { timeout: 10000 });
  });

  test('OnFinished load more', async function(assert): Promise<void> {
    assert.expect(5);
    let finished = false;
    await renderList(this, {
      nItems: testNItems,
      chunkSize: 11,
      onFinished: (currentIndex: number) => {
        if (currentIndex === testNItems) {
          assert.equal(currentIndex, testNItems);
          return Promise.resolve(['hola']);
        } else if (currentIndex === testNItems + 1) {
          assert.equal(currentIndex, testNItems + 1);
          return Promise.resolve(['adios']);
        } else {
          assert.equal(currentIndex, testNItems + 2);
          finished = true;
          return Promise.resolve([]);
        }
      }
    });

    await waitFor('.number', { timeout: 10000, count: testNItems + 1 });
    assert.dom(`.number:nth-child(${testNItems + 1})`).hasText(`hola`);

    await waitFor('.number', { timeout: 10000, count: testNItems + 2 });
    assert.dom(`.number:nth-child(${testNItems + 2})`).hasText(`adios`);

    await waitUntil(() => finished, { timeout: 10000 });
  });
});
