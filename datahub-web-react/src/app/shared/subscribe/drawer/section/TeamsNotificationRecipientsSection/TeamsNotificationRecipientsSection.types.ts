import { TeamsSearchResult } from '@app/shared/subscribe/drawer/teams-search-client';

export type SearchOptionData = {
    value: string;
    label: string;
    description: string;
};

export type TeamsSearchProps = {
    selectedType: 'user' | 'channel';
    onTypeChange: (type: 'user' | 'channel') => void;
    onSearch: (query: string) => void;
    onSelectOption: (value: string) => void;
    searchOptions: SearchOptionData[];
    selectedOptionValues: string[];
    teamsSinkSupported: boolean;
};

export type TeamsSaveDefaultProps = {
    isChecked: boolean;
    onToggle: (checked: boolean) => void;
    settingsTeamsChannel?: string;
    selectedOption?: TeamsSearchResult | null;
    selectedResult?: TeamsSearchResult | null;
    subscriptionChannel?: string;
};

export type TeamsDisabledProps = {
    isAdminAccess: boolean;
};

export type TeamsWarningProps = {
    show: boolean;
};
