import {
  ISuggestionBuilder,
  IState,
  AutocompleteRuleNames,
  INodeProcessor
} from 'datahub-web/utils/parsers/autocomplete/types';
import { entityProcessor } from 'datahub-web/utils/parsers/autocomplete/processors/entity';
import { facetsProcessor } from 'datahub-web/utils/parsers/autocomplete/processors/facets';
import { logicalOperatorsProcessor } from 'datahub-web/utils/parsers/autocomplete/processors/logical-operators';
import { arrayReduce } from '@datahub/utils/array/index';

/**
 * List of grammarnode-processs to process the nodes that provides value for the autocomplete
 */
const nodesProcessors: INodeProcessor = {
  ...logicalOperatorsProcessor,
  ...entityProcessor,
  ...facetsProcessor
};

/**
 * We will look if there is some processing to do with that grammar node and call process it
 * @param {ISuggestionBuilder} builder instance of the grammar node to process
 * @param {IState} ruleState the state of the current rules
 * @returns Promise<ISuggestionBuilder>
 */
const processRule = async (builder: ISuggestionBuilder, ruleState: IState): Promise<ISuggestionBuilder> => {
  const ruleName = ruleState.rule.name as keyof typeof AutocompleteRuleNames;
  const processor = nodesProcessors[ruleName];

  return processor ? await processor(builder, ruleState) : builder;
};

/**
 * Once we have the suggestions we will call process rule for every 'interesting rule' that
 * we find.
 *
 * After that we will create the categories in the display order.
 *
 * @param {ISuggestionBuilder} builder
 * @returns {Promise<ISuggestionBuilder>}
 */
export const processRules = (builder: ISuggestionBuilder): Promise<ISuggestionBuilder> => {
  const { wantedRulesMap } = builder;
  const wantedRuleNames = Object.keys(wantedRulesMap);

  return arrayReduce(
    async (builder: Promise<ISuggestionBuilder>, ruleName: string) =>
      await processRule(await builder, wantedRulesMap[ruleName]),
    Promise.resolve(builder)
  )(wantedRuleNames);
};
