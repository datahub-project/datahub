import { AutocompleteDropdownAlign } from '@src/alchemy-components/components/AutoComplete/types';

// Adjusted aligning to show dropdown in the correct place
export const AUTOCOMPLETE_DROPDOWN_ALIGN: AutocompleteDropdownAlign = {
    // bottom-center of input (search) and top-center of dropdown
    points: ['bc', 'tc'],
    // additional offset
    offset: [22, -4],
};

export const EXACT_AUTOCOMPLETE_OPTION_TYPE = 'exact_query';
export const RELEVANCE_QUERY_OPTION_TYPE = 'recommendation';

export const DEBOUNCE_ON_SEARCH_TIMEOUT_MS = 100;
