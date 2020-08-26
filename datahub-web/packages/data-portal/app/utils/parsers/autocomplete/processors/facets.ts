import {
  INodeProcessor,
  AutocompleteRuleNames,
  ISuggestionBuilder,
  IState,
  INodeFacetProcessor,
  ISuggestion
} from 'datahub-web/utils/parsers/autocomplete/types';
import { dataToString } from 'datahub-web/utils/parsers/autocomplete/utils';
import { platform } from 'datahub-web/utils/parsers/autocomplete/processors/facets/platform';
import { fabric } from 'datahub-web/utils/parsers/autocomplete/processors/facets/fabric';
import { pii, tier, frequency, highPriority } from 'datahub-web/utils/parsers/autocomplete/processors/facets/metrics';
import { fetchFacetValue } from 'datahub-web/utils/parsers/helpers';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/search/search-entity-render-prop';

/**
 * Helper fn to get the facet name from a FacetValue grammar rule
 * @param state FacetValue grammar rule
 */
const getFacetNameFromStateRule = (state: IState): string => {
  //isNodeFacetValue
  if (
    state.rule.name === AutocompleteRuleNames.FacetValue &&
    state.wantedBy[0] &&
    state.wantedBy[0].left &&
    state.wantedBy[0].left.right &&
    state.wantedBy[0].left.right.data
  ) {
    return dataToString(state.wantedBy[0].left.right.data);
  }

  // isNodeFacetName
  if (state.rule.name === AutocompleteRuleNames.FacetName && state.from && state.from.from && state.from.from.right) {
    return dataToString(state.from.from.right.data);
  }

  return '';
};

/**
 * Helper fn to get the facet value from a FacetValue grammar rule
 * @param state FacetValue grammar rule
 */
const getFacetValueFromStateRule = (state: IState): string => {
  // isNodeFacetValue
  if (state.rule.name === AutocompleteRuleNames.FacetValue && state.from && state.from.from && state.from.from.right) {
    return dataToString(state.from.from.right.data);
  }

  return '';
};

/**
 * Current facets processors available
 * @type {INodeFacetProcessor}
 */
export const facetNameProcessor: INodeFacetProcessor = {
  platform,
  dataorigin: fabric,
  highPriority,
  pii,
  tier,
  frequency
};

export const facetsProcessor: INodeProcessor = {
  /**
   * When 'name' is expected we just return 'name:' as suggestion
   */
  [AutocompleteRuleNames.FacetName]: (builder: ISuggestionBuilder, ruleState: IState): Promise<ISuggestionBuilder> => {
    const allFields: Array<ISearchEntityRenderProps> = builder.entity.renderProps.search.attributes;
    const facetName = getFacetNameFromStateRule(ruleState);
    const fields = allFields.filter(
      (field): boolean => field.fieldName.indexOf(facetName) >= 0 && field.showInAutoCompletion
    );
    return Promise.resolve({
      ...builder,
      facetNames: [
        ...builder.facetNames,
        ...fields.map(
          (field): ISuggestion => ({
            title: `${field.fieldName}:`,
            text: `${builder.textPrevious}${field.fieldName}:`,
            description: `${field.desc}, e.g.: ${field.example}`
          })
        )
      ]
    });
  },

  /**
   * When facet value is expected, we drill it down as depending on which one we would like
   * to do one thing or another. For example, when facet is name, we need to fetch dataset names
   * from the backend, but for other cases (fabric) we can do just a local UI search
   *
   * TODO META-6999: make facets dynamic
   */
  [AutocompleteRuleNames.FacetValue]: async (
    builder: ISuggestionBuilder,
    ruleState: IState
  ): Promise<ISuggestionBuilder> => {
    const facetName = getFacetNameFromStateRule(ruleState);
    const facetValue = getFacetValueFromStateRule(ruleState);

    // First lets check if we have a special processing for that property
    const processor = facetNameProcessor[facetName];

    if (processor) {
      return await processor(builder, facetValue);
    }

    const suggestions = await fetchFacetValue(facetName, facetValue, builder.entity);

    return {
      ...builder,
      facetNames: [
        ...builder.facetNames,
        ...(suggestions || []).map(
          (value): ISuggestion => ({
            title: `${facetName}:${value}`,
            text: `${builder.textPrevious}${facetName}:${value} `
          })
        )
      ]
    };
  }
};
