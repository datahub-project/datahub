import {
  AutocompleteRuleNames,
  ISuggestionBuilder,
  INodeProcessor
} from 'datahub-web/utils/parsers/autocomplete/types';

export const logicalOperatorsProcessor: INodeProcessor = {
  /**
   * When 'or' is expected, then suggest 'OR'
   */
  [AutocompleteRuleNames.OrOperator]: (builder: ISuggestionBuilder): Promise<ISuggestionBuilder> =>
    Promise.resolve({
      ...builder,
      logicalOperators: [
        ...builder.logicalOperators,
        {
          title: 'OR',
          text: `${builder.textPrevious}OR `
        }
      ]
    }),

  /**
   * When 'and' is expected, then suggest 'AND'
   */
  [AutocompleteRuleNames.AndOperator]: (builder: ISuggestionBuilder): Promise<ISuggestionBuilder> =>
    Promise.resolve({
      ...builder,
      logicalOperators: [
        ...builder.logicalOperators,
        {
          title: 'AND',
          text: `${builder.textPrevious}AND `
        }
      ]
    })
};
