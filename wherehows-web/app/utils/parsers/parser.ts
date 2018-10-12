import { CompiledRules } from 'nearley';
import nearley from 'nearley';
import { Parser } from 'wherehows-web/typings/modules/nearley';

/**
 * We will preprocess the string to remove extraspaces that will slow down the parser.
 * @param text
 * @param grammar
 */
export const parse = (text: string, grammar: CompiledRules): Parser => {
  const noExtraSpaces = text.replace(/[\s]+/gi, ' ').trim();
  const parser = new nearley.Parser(nearley.Grammar.fromCompiled(grammar));
  parser.feed(noExtraSpaces);
  return parser;
};
