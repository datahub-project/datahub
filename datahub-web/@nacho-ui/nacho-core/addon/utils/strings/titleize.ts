import { capitalize } from '@ember/string';
import { IObject } from '@nacho-ui/core/types/utils/generics';

export interface ITitleizeOptions {
  // Useful for when we have unique items in the given string that should be treated as separate words.
  // Ex: givenUMPDataset => UMP is a whole word, but our titleize would normally treat it as 3 camelcase words
  defineWords?: Array<string>;
}

/**
 * We are not given defined words, then titleize will default to a subset of known acronyms
 */
const defaultDefinedWords = ['AI', 'ML'];

/**
 * Given a string value, whose format can be dasherized, camelcase, or underscore, turn this into a proper
 * display format.
 * @param value - given string to titleize
 * @param options - Options that improve the intelligence of our titleize function
 */
export default function titleize(value: string, options: ITitleizeOptions = {}): string {
  if (typeof value !== 'string') {
    return value;
  }

  const definedWords = options.defineWords || defaultDefinedWords;
  let definedMap: IObject<string>;

  // If we have a list of defined words, we want to create a way to identify these whole words separately
  // from the rest of the string in the split logic below using an existing separator, _
  if (definedWords) {
    definedMap = definedWords.reduce((mapping, word) => {
      mapping[word] = ` ${word} `;
      return mapping;
    }, {} as IObject<string>);
    const regexTester = new RegExp(definedWords.join('|'), 'gi');
    // Expectation: definedWords = ['POKEMON'], in the string electricPOKEMONBoogaloo =>
    // electric_POKEMON_Boogaloo
    value = value.replace(regexTester, function(matched) {
      // Fixes scenarios where we have an acronym that has been lower-cased that needs to be titleized
      return definedMap[matched] || definedMap[matched.toUpperCase()];
    });
  }

  return value
    .trim()
    .split(/[ _-]+/)
    .map(word => {
      let idx = 0;
      let words = word[idx];

      // If the user has defined this specific word as an item, we leave it alone
      if (definedWords && definedMap[word]) {
        return word;
      }

      for (idx = 1; idx < word.length; idx++) {
        // Handling camelcase separation of words
        words += /[A-Z]/.test(word[idx]) ? ' ' + word[idx] : word[idx];
      }

      return words && capitalize(words);
    })
    .join(' ');
}
