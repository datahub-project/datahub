import { helper } from '@ember/component/helper';

const tenseExceptions: Record<string, string> = {
  are: 'were',
  eat: 'ate',
  go: 'went',
  have: 'had',
  is: 'was',
  run: 'ran',
  sit: 'sat',
  inherit: 'inherited',
  visit: 'visited'
};

/**
 * Given a verb string, return the probable past tense version of the verb
 * @param verb - An assumed verb given to the function
 */
export function pastTense([verb]: [string]): string {
  if (typeof verb !== 'string') {
    return verb;
  }
  // Exceptions take highest priority since we already know what they should look like
  if (tenseExceptions[verb]) {
    return tenseExceptions[verb];
  }
  // If the word ends in 'e', generally we do not need to add another 'e' before 'd'
  if (/e$/i.test(verb)) {
    return verb + 'd';
  }
  // If the word ends in 'c', generally that needs 'ked', example 'magic' => 'magicked'
  if (/[aeiou]c/i.test(verb)) {
    return verb + 'ked';
  }

  // The next two are same as base case, but take precedence over the last rule

  // For american english only, words that end in 'el' don't double up on the consonant before 'ed'
  if (/el$/i.test(verb)) {
    return verb + 'ed';
  }
  // Also any word that ends in 2 vowels before a consonant, we don't double up on the consonant
  // before 'ed'
  if (/[aeio][aeiou][dlmnprst]$/.test(verb)) {
    return verb + 'ed';
  }
  // If the above 2 rules are not applicable, we end all words that end in a vowel and certain
  // consonants with a double consonant + 'ed'
  if (/[aeiou][bdglmnprst]$/i.test(verb)) {
    return verb.replace(/(.+[aeiou])([bdglmnprst])/, '$1$2$2ed');
  }

  // Base case
  return verb + 'ed';
}

export default helper(pastTense);
