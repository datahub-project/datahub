import { ISuggestionBuilder, AutocompleteRuleNames } from 'datahub-web/utils/parsers/autocomplete/types';

/**
 * Will clean up the map from the last step (04-create-wanted-rules-map) and leave the ones that
 * provide value for the autocomplete.
 * @param builder
 */
export const filterWantedRulesMap = (builder: ISuggestionBuilder): ISuggestionBuilder => {
  const wantedRulesMap = Object.keys(AutocompleteRuleNames).reduce((autocomplete, ruleName) => {
    if (builder.allWantedRulesMap[ruleName]) {
      return { ...autocomplete, [ruleName]: builder.allWantedRulesMap[ruleName] };
    }
    return autocomplete;
  }, {});
  return {
    ...builder,
    wantedRulesMap
  };
};
