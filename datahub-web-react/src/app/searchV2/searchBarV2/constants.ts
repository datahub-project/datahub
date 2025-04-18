import { AutocompleteDropdownAlign } from '@src/alchemy-components/components/AutoComplete/types';

// Adjusted aligning to show dropdown in the correct place
export const AUTOCOMPLETE_DROPDOWN_ALIGN_WITH_NEW_NAV_BAR: AutocompleteDropdownAlign = {
    // bottom-center of input (search) and top-center of dropdown
    points: ['bl', 'tl'],
    // additional offset
    offset: [0, 6],
};
export const AUTOCOMPLETE_DROPDOWN_ALIGN: AutocompleteDropdownAlign = {
    // bottom-center of input (search) and top-center of dropdown
    points: ['bl', 'tl'],
    // additional offset
    offset: [0, -2],
};

export const EXACT_AUTOCOMPLETE_OPTION_TYPE = 'exact_query';
export const RELEVANCE_QUERY_OPTION_TYPE = 'recommendation';

export const DEBOUNCE_ON_SEARCH_TIMEOUT_MS = 100;

export const BOX_SHADOW = `0px -3px 12px 0px rgba(236, 240, 248, 0.5) inset,
0px 3px 12px 0px rgba(255, 255, 255, 0.5) inset,
0px 20px 60px 0px rgba(0, 0, 0, 0.12)`;
