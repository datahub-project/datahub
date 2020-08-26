import { CompiledRules } from 'nearley';
import nearley from 'nearley';
import { Parser } from 'datahub-web/typings/modules/nearley';

/**
 * We will pre-process the string to remove extra spaces that will slow down the parser.
 * @param text
 * @param grammar
 */
export const parse = (text: string, grammar: CompiledRules): Parser => {
  const noExtraSpaces = text.replace(/[\s]+/gi, ' ').trim();
  const parser = new nearley.Parser(nearley.Grammar.fromCompiled(grammar));
  const withLastSpace = text.endsWith(' ') ? `${noExtraSpaces} ` : noExtraSpaces;
  parser.feed(withLastSpace);
  return parser;
};
