import { IGrammarProcessFn, ISuggestionBuilder, ISuggestionGroup } from 'datahub-web/utils/parsers/autocomplete/types';
import { init } from 'datahub-web/utils/parsers/autocomplete/steps/init';
import { processText } from 'datahub-web/utils/parsers/autocomplete/steps/process-text';
import { feed } from 'datahub-web/utils/parsers/autocomplete/steps/feed';
import { createWantedRulesMap } from 'datahub-web/utils/parsers/autocomplete/steps/create-wanted-rules-map';
import { filterWantedRulesMap } from 'datahub-web/utils/parsers/autocomplete/steps/filter-wanted-rules-map';
import { processRules } from 'datahub-web/utils/parsers/autocomplete/steps/process-rules';
import { generateGroups } from 'datahub-web/utils/parsers/autocomplete/steps/generate-groups';
import { arrayReduce } from '@datahub/utils/array/index';
import { createSuggestionsFromError } from 'datahub-web/utils/parsers/helpers';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { DataModelEntity } from '@datahub/data-models/constants/entity';

/**
 * Steps of the grammar process
 * @type {Array<IGrammarProcessFn>}
 */
export const grammarProcessingSteps: Array<IGrammarProcessFn> = [
  processText,
  feed,
  createWantedRulesMap,
  filterWantedRulesMap,
  processRules,
  generateGroups
];

/**
 * Takes a query text and a related entity to filter by and traverses the resulting ISuggestionBuilder object
 * through each step.
 * This builds a list of suggestions grouped by category, Array<ISuggestionGroup>
 * Steps are a list of IGrammarProcessFn functions
 * @param {string} query the query string to search for suggested entities
 * @param {Entity} [entity=Entity.DATASET] the grouping of entities for the query, i.e. entity to filter by
 * @param {Array<IGrammarProcessFn>} steps a list of steps to sequentially process the suggestion builder
 * @returns {Promise<Array<ISuggestionGroup>>}
 */
export const typeaheadQueryProcessor = async (
  query: string,
  entity: DataModelEntity = DatasetEntity,
  steps: Array<IGrammarProcessFn>
): Promise<Array<ISuggestionGroup>> => {
  const initArgs: Pick<ISuggestionBuilder, 'text' | 'entity'> = { text: query, entity };
  const initialBuilder: Promise<ISuggestionBuilder> = Promise.resolve(init(initArgs));

  try {
    // Asynchronously performs each step in sequence to generate the suggestionBuilder
    const suggestionBuilder: ISuggestionBuilder = await arrayReduce(
      async (builder: Promise<ISuggestionBuilder>, stepFun: IGrammarProcessFn): Promise<ISuggestionBuilder> =>
        await stepFun(await builder),
      initialBuilder
    )(steps);
    return suggestionBuilder.groups;
  } catch (e) {
    return createSuggestionsFromError('There was an unexpected error, please try again later.');
  }
};
