import {
  INodeProcessor,
  AutocompleteRuleNames,
  ISuggestionBuilder,
  ISuggestion
} from 'datahub-web/utils/parsers/autocomplete/types';
import { fetchFacetValue } from 'datahub-web/utils/parsers/helpers';

/**
 * When dataset node is encounter, we will process it by making an api call to the backend to fetch
 * datasets with that name.
 */
export const entityProcessor: INodeProcessor = {
  [AutocompleteRuleNames.EntityName]: async (builder: ISuggestionBuilder): Promise<ISuggestionBuilder> => {
    const input = builder.textLastWord || '';
    const nameField = builder.entity.renderProps.search.autocompleteNameField || 'name';
    const suggestions = await fetchFacetValue(nameField, input, builder.entity);

    return {
      ...builder,
      datasets: [
        ...builder.datasets,
        ...(suggestions || []).map(
          (entityName): ISuggestion => ({
            title: entityName,
            text: `${builder.textPrevious}${entityName} `,
            description: ''
          })
        )
      ]
    };
  }
};
