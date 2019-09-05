import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click, triggerEvent } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { setProperties } from '@ember/object';
import { TestContext } from 'ember-test-helpers';
import { querySelector, querySelectorAll, getTextNoSpaces } from '@datahub/utils/test-helpers/dom-helpers';
import { typeInSearch } from 'ember-power-select/test-support/helpers';
import { ISuggestionGroup } from 'wherehows-web/utils/parsers/autocomplete/types';
import { setMockConfig, resetConfig } from 'wherehows-web/services/configurator';
import { DataModelName } from '@datahub/data-models/constants/entity';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

interface ISearchBoxContract {
  selectedEntity: DataModelName;
  text?: string;
  onTypeahead: { perform: () => Array<ISuggestionGroup> };
  onSearch: (text: string, entity?: DataModelName) => void;
}

interface ISearchBoxTestContext extends TestContext, ISearchBoxContract {}

const inputSelector = '.ember-power-select-search-input';
const optionSelector = '.ember-power-select-option';
const searchButtonSelector = '.nacho-global-search__button';

const getInput = (test: ISearchBoxTestContext): HTMLInputElement => {
  return querySelector<HTMLInputElement>(test, inputSelector);
};

const focusInput = async () => {
  await click(inputSelector); // Focus
  await triggerEvent(inputSelector, 'focus');
};

const blurInput = async () => {
  await click('button'); // Blur
  await triggerEvent(inputSelector, 'blur');
};

const getSearchOptions = (): Array<ISuggestionGroup> => [
  { groupName: 'groupName', options: [{ text: 'one', title: 'one' }] },
  { groupName: 'groupName', options: [{ text: 'two', title: 'two' }] }
];

const setTestProps = (test: ISearchBoxTestContext, options: Partial<ISearchBoxContract> = {}) => {
  const props: ISearchBoxContract = {
    selectedEntity: DatasetEntity.displayName,
    text: undefined,
    onTypeahead: {
      perform() {
        return [];
      }
    },
    onSearch: () => {},
    ...options
  };

  setProperties(test, props);

  return props;
};

const getBaseTest = async (test: ISearchBoxTestContext, override: Partial<ISearchBoxContract> = {}) => {
  const testProps = setTestProps(test, override);

  await render(hbs`
    {{#search/search-box
      selectedEntity=selectedEntity
      text=(readonly text)
      onTypeahead=onTypeahead
      onSearch=(action onSearch) as |suggestion|}}
      {{suggestion}}
    {{/search/search-box}}
  `);

  return testProps;
};

module('Integration | Component | search/search-box', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    setMockConfig({ showPeople: false });
  });

  hooks.afterEach(function() {
    resetConfig();
  });

  test('it renders', async function(this: ISearchBoxTestContext, assert) {
    await getBaseTest(this);

    assert.equal(getTextNoSpaces(this), 'SearchSelectentitytypeDatasets');
    assert.equal(getInput(this).value, '');
  });

  test('input shows text', async function(this: ISearchBoxTestContext, assert) {
    const props = await getBaseTest(this, { text: 'search this' });

    assert.equal(getTextNoSpaces(this), 'SearchSelectentitytypeDatasets');
    assert.equal(getInput(this).value, props.text);
  });

  test('input shows text, and it does not lose state', async function(this: ISearchBoxTestContext, assert) {
    const word = 'Hola carambola';
    await getBaseTest(this);

    const input = getInput(this);

    await focusInput();
    await typeInSearch(word);
    assert.equal(input.value, word);

    await blurInput();
    assert.equal(input.value, word);

    await focusInput();
    assert.equal(input.value, word);
  });

  test('show list when text present and focus', async function(this: ISearchBoxTestContext, assert) {
    const searchOptions: Array<ISuggestionGroup> = getSearchOptions();
    await getBaseTest(this, {
      text: 'search this',
      onTypeahead: { perform: () => searchOptions }
    });

    await focusInput();
    const domOptions = querySelectorAll(this, optionSelector);

    assert.equal(domOptions.length, 2, 'options should show up');
    searchOptions.forEach((suggestionGroup: ISuggestionGroup, index: number) => {
      const content = domOptions[index].textContent || '';
      assert.equal(content.trim(), suggestionGroup.options[0].text);
    });

    await blurInput();
    const domOptionsGone = querySelectorAll(this, optionSelector);
    assert.equal(domOptionsGone.length, 0, 'options should hide');
  });

  test('search button triggers search with actual text', async function(this: ISearchBoxTestContext, assert) {
    const expectedSearch = 'expected search';
    let searchedWord = '';
    const onSearch = (word: string) => {
      searchedWord = word;
    };

    await getBaseTest(this, { text: expectedSearch, onSearch });
    await click(searchButtonSelector);

    assert.equal(searchedWord, expectedSearch);
  });

  test('request not completed, will be cancelled on blur', async function(this: ISearchBoxTestContext, assert) {
    let cancelled = false;
    // Does not ever resolve
    const onTypeahead = { perform: () => [] };

    const onTypeaheadTask = {
      cancelAll: () => {
        cancelled = true;
      }
    };

    setTestProps(this, { onTypeahead });
    this.set('onTypeaheadTask', onTypeaheadTask);

    await render(hbs`
    {{#search/search-box
      selectedEntity=selectedEntity
      text=(readonly text)
      onTypeahead=onTypeahead
      onTypeaheadTask=onTypeaheadTask
      onSearch=(action onSearch) as |suggestion|}}
      {{suggestion}}
    {{/search/search-box}}
  `);

    await blurInput();

    assert.ok(cancelled);
  });

  test('when suggestions opens, no suggestion is selected', async function(this: ISearchBoxTestContext, assert) {
    const searchOptions = getSearchOptions();
    await getBaseTest(this, { text: 'search this', onTypeahead: { perform: () => searchOptions } });
    await focusInput();

    assert.equal(querySelectorAll(this, '.ember-power-select-option[aria-current=true]').length, 0);
  });

  test('click on suggestion should work', async function(this: ISearchBoxTestContext, assert) {
    const expectedSearch = 'expected search';
    const searchOptions = getSearchOptions();
    await getBaseTest(this, { text: expectedSearch, onTypeahead: { perform: () => searchOptions } });
    await focusInput();

    await click('.ember-power-select-option:first-child');

    const input = getInput(this);

    assert.equal(input.value, searchOptions[0].options[0].text);
    assert.equal(document.activeElement, input);
  });

  test('pressing enter should trigger search and suggestion box should go away', async function(this: ISearchBoxTestContext, assert) {
    const searchOptions = getSearchOptions();
    const expectedSearch = 'expected search';
    let searchedWord = '';
    const onSearch = (word: string) => {
      searchedWord = word;
    };

    await getBaseTest(this, { text: expectedSearch, onSearch, onTypeahead: { perform: () => searchOptions } });
    await focusInput();
    await triggerEvent('form', 'submit');
    await assert.equal(searchedWord, expectedSearch);

    assert.equal(querySelectorAll(this, '.ember-power-select-option').length, 0);
  });

  test('search gives us the right entity back', async function(this: ISearchBoxTestContext, assert) {
    const expectedSearch = 'expected search';
    let onSearchSearchTerm;
    let onSearchEntity;

    const onSearch = (searchTerm: string, entity: DataModelName) => {
      onSearchSearchTerm = searchTerm;
      onSearchEntity = entity;
    };

    await getBaseTest(this, { text: expectedSearch, onSearch });
    await click(searchButtonSelector);

    assert.equal(onSearchSearchTerm, expectedSearch);
    assert.equal(onSearchEntity, DatasetEntity.displayName);
  });
});
