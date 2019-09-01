import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { ISearchFacet, IFacetSelections, IFacetCounts } from '@datahub/data-models/types/entity/facets';
import { getTextNoSpaces } from '@datahub/utils/test-helpers/dom-helpers';
import { TestContext } from 'ember-test-helpers';

const CLEAR_BTN_SELECTOR = '.search-facet__clear-btn';
const FACET_OPTION_SELECTOR = '.search-facet__option label';

const createTest = async (test: TestContext, selections: IFacetSelections = {}) => {
  const facet: ISearchFacet = {
    displayName: 'Facet 1',
    name: 'facet1',
    values: [
      {
        label: 'Value 1',
        value: 'value1',
        count: 1
      },
      {
        label: 'Value 2',
        value: 'value2',
        count: 10
      }
    ]
  };

  const counts: IFacetCounts = {
    value1: 1,
    value2: 10
  };

  let clearCalled = false;
  let changeCalled = false;

  const onFacetChange = () => {
    changeCalled = true;
  };

  const onFacetClear = () => {
    clearCalled = true;
  };

  test.setProperties({
    facet,
    selections,
    counts,
    onFacetChange,
    onFacetClear
  });

  await render(hbs`{{search/search-facet
    facet=facet
    selections=selections
    counts=counts
    onChange=(action onFacetChange)
    onClear=(action onFacetClear)}}`);

  return {
    wasClearCalled: () => clearCalled,
    wasChangedCalled: () => changeCalled
  };
};

module('Integration | Component | search/search-facet', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await createTest(this);
    assert.equal(getTextNoSpaces(this), 'Facet1Value11Value210');
  });

  test('it renders with clear', async function(assert) {
    await createTest(this, { value1: true });
    assert.equal(getTextNoSpaces(this), 'Facet1clearValue11Value210');
  });

  test('clear action', async function(assert) {
    const { wasClearCalled } = await createTest(this, { value1: true });
    assert.equal(getTextNoSpaces(this), 'Facet1clearValue11Value210');
    await click(CLEAR_BTN_SELECTOR);
    assert.ok(wasClearCalled(), 'Clear was called');
  });

  test('facet changed action', async function(assert) {
    const { wasChangedCalled } = await createTest(this, { value1: true });
    assert.equal(getTextNoSpaces(this), 'Facet1clearValue11Value210');
    await click(FACET_OPTION_SELECTOR);
    assert.ok(wasChangedCalled(), 'Clear was called');
  });
});
