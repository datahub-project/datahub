import { ISuggestionBuilder } from 'wherehows-web/utils/parsers/autocomplete/types';
import autocomplete from 'wherehows-web/parsers/autocomplete';
import { parse } from 'wherehows-web/utils/parsers/parser';
import { createSuggestionsFromError } from 'wherehows-web/utils/parsers/helpers';

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
