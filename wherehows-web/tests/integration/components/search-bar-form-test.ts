import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { querySelector } from 'wherehows-web/tests/helpers/dom-helpers';
import {
  searchBarSelector,
  searchButton,
  searchSuggestions
} from 'wherehows-web/tests/helpers/search/global-search-constants';
import { TestContext } from 'ember-test-helpers';

const createSearchTest = () => {
  let searchPerformed = false;
  const testObj = {
    keyword: 'car',
    didSearch: () => {
      searchPerformed = true;
    },
    wasSearchPerformed: () => {
      return searchPerformed;
    }
  };
  return testObj;
};

/**
 * Will setup search test: create mock data, render and query dom
 * @param test current test
 */
const setupSearchTest = async (test: TestContext) => {
  const testObj = createSearchTest();
  test.setProperties(testObj);

  await render(hbs`{{search-bar-form selection=keyword didSearch=(action didSearch)}}`);
  const input = querySelector<HTMLInputElement>(test, searchBarSelector);
  const suggestions = querySelector(test, searchSuggestions);

  if (!input) {
    throw new Error('input should not be null');
  }

  if (!suggestions) {
    throw new Error('suggestions should not be null');
  }

  return { input, suggestions, testObj };
};

module('Integration | Component | search-bar-form', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    const { input, testObj } = await setupSearchTest(this);

    assert.notEqual(input, null, 'Input is not null');
    assert.equal(input.value, testObj.keyword);
  });

  test('blur does not trigger search', async function(assert) {
    const { input, testObj } = await setupSearchTest(this);

    await input.focus();
    await input.blur();

    assert.notOk(testObj.wasSearchPerformed(), 'Search should not be triggered');
  });

  test('suggestions box hides after search', async function(assert) {
    const { input, testObj, suggestions } = await setupSearchTest(this);

    await input.focus();

    assert.ok((suggestions.classList.contains('tt-open'), 'Suggestions must be open'));

    await click(searchButton);

    assert.ok(testObj.wasSearchPerformed(), 'Search was performed');
    assert.notOk(suggestions.classList.contains('tt-open'), 'Suggestion box should not be open after search');
  });
});
