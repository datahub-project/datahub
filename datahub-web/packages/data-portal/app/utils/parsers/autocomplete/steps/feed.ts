import { ISuggestionBuilder } from 'datahub-web/utils/parsers/autocomplete/types';
import autocomplete from 'datahub-web/parsers/autocomplete';
import { parse } from 'datahub-web/utils/parsers/parser';
import { createSuggestionsFromError } from 'datahub-web/utils/parsers/helpers';

/**
 * This step will call the parser with the text, and save the results in the builder.
 * if the text is invalid, it will continue but suggestion will be made
 * @param builder
 */
export const feed = (builder: ISuggestionBuilder): ISuggestionBuilder => {
  if (typeof builder.text === 'undefined') {
    return builder;
  }

  try {
    const parser = parse(builder.text, autocomplete);
    return {
      ...builder,
      parser
    };
  } catch (e) {
    return {
      ...builder,
      groups: createSuggestionsFromError('There was a problem parsing your search query')
    };
  }
};
