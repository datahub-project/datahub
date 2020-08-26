import { StateNodeMap, IState, ISuggestionBuilder } from 'datahub-web/utils/parsers/autocomplete/types';

/**
 * Will explore the expecte tree moving the nodes in a map structure, also, preventing infinite recursion.
 * @param builder
 */
export const createWantedRulesMap = (builder: ISuggestionBuilder): ISuggestionBuilder => {
  if (!builder.parser || !builder.parser.table) {
    return builder;
  }

  const allWantedRulesMap: StateNodeMap = {};
  const inspect = (rules: Array<IState>, from?: IState) => {
    rules.forEach(rule => {
      if (!allWantedRulesMap[rule.rule.name]) {
        const detailedRule = { ...rule, from };
        allWantedRulesMap[rule.rule.name] = detailedRule;
        inspect(rule.wantedBy, detailedRule);
      }
    });
  };

  inspect(builder.parser.table[builder.parser.current].scannable);

  return {
    ...builder,
    allWantedRulesMap
  };
};
