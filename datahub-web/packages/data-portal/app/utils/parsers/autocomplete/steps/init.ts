import { ISuggestionBuilder } from 'datahub-web/utils/parsers/autocomplete/types';

/**
 * Initialize a suggestion builder structure with a text
 * @param {(Pick<ISuggestionBuilder, 'text' | 'entity'>)} { text, entity }
 * @returns {ISuggestionBuilder}
 */
export const init = ({ text, entity }: Pick<ISuggestionBuilder, 'text' | 'entity'>): ISuggestionBuilder => ({
  logicalOperators: [],
  facetNames: [],
  datasets: [],
  wantedRulesMap: {},
  allWantedRulesMap: {},
  groups: [],
  text,
  entity
});
