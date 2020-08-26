import { ISuggestionBuilder } from 'datahub-web/utils/parsers/autocomplete/types';

/**
 * In order to give suggestions we need to process the text to remove the last work
 * so we can autocomplete the rest correctly. Also, we will save a reference to the last word.
 * @param builder
 */
export const processText = (builder: ISuggestionBuilder): ISuggestionBuilder => {
  if (typeof builder.text === 'undefined') {
    return builder;
  }

  const splitText = builder.text.split(' ');
  const previousTextWithSpace = splitText.slice(0, splitText.length - 1).join(' ');

  return {
    ...builder,
    textPrevious: `${previousTextWithSpace}${previousTextWithSpace.length > 0 ? ' ' : ''}`,
    textLastWord: splitText[splitText.length - 1]
  };
};
