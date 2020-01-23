import {
  INodeProcessor,
  AutocompleteRuleNames,
  ISuggestionBuilder
} from 'wherehows-web/utils/parsers/autocomplete/types';
import { fetchFacetValue } from 'wherehows-web/utils/parsers/helpers';

/**
 * When dataset node is encounter, we will process it by making an api call to the backend to fetch
 * datasets with that name.
 */
export const entityProcessor: INodeProcessor = {
  [AutocompleteRuleNames.EntityName]: async (builder: ISuggestionBuilder): Promise<ISuggestionBuilder> => {
    const input = builder.textLastWord || '';
    const suggestions = await fetchFacetValue('name', input, builder.entity);

    return {
      ...builder,
      datasets: [
        ...builder.datasets,
        ...(suggestions || []).map(entityName => ({
          title: entityName,
          text: `${builder.textPrevious}${entityName} `,
          description: ''
        }))
      ]
    };
  }
};
