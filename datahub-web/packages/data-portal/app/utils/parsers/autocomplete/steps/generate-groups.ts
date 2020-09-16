import {
  ISuggestionGroup,
  ISuggestionBuilder,
  AutocompleteRuleNames
} from 'datahub-web/utils/parsers/autocomplete/types';
import titleize from '@nacho-ui/core/utils/strings/titleize';

/**
 * Will generate suggestion groups
 * @param builder
 */
export const generateGroups = (builder: ISuggestionBuilder): ISuggestionBuilder => {
  const groups: Array<ISuggestionGroup> = [];
  const isEntityNamesEmpty = builder.datasets.length === 0;
  const expectedEntityName = !!builder.wantedRulesMap[AutocompleteRuleNames.EntityName];
  const lastWordLength = typeof builder.textLastWord === 'string' ? builder.textLastWord.trim().length : -1;
  const entityModel = builder.entity;
  const entityDisplayName = titleize(entityModel.displayName);

  if (isEntityNamesEmpty && expectedEntityName && lastWordLength < 3) {
    groups.push({
      groupName: entityDisplayName,
      options: [
        {
          title: `type at least ${3 - lastWordLength} more characters to see ${entityDisplayName} names`,
          text: '',
          disabled: true
        }
      ]
    });
  }

  if (builder.logicalOperators.length > 0) {
    groups.push({ groupName: 'Operators', options: builder.logicalOperators });
  }

  if (builder.facetNames.length > 0) {
    groups.push({ groupName: 'Filter By', options: builder.facetNames });
  }

  if (builder.datasets.length > 0) {
    groups.push({ groupName: entityDisplayName, options: builder.datasets });
  }

  return {
    ...builder,
    groups
  };
};
