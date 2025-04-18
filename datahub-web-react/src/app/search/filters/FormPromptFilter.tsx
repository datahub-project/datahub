import React, { useMemo, useState } from 'react';
import { QuestionCircleOutlined } from '@ant-design/icons';
import SearchFilterView from './SearchFilterView';
import {
    FORM_RESPONSES_FILTER,
    FormResponsesFilter,
    useEntityFormContext,
} from '../../entity/shared/entityForm/EntityFormContext';
import { FilterOptionType } from './types';
import FilterOption from './FilterOption';

export default function FormPromptFilter() {
    const {
        filter: { formResponsesFilters, setFormResponsesFilters },
    } = useEntityFormContext();
    const initialSelectedFilterOptions = useMemo(
        () => formResponsesFilters?.map((filter) => ({ field: FORM_RESPONSES_FILTER, value: filter })) || [],
        [formResponsesFilters],
    );
    const [selectedFilterOptions, setSelectedFilterOptions] =
        useState<FilterOptionType[]>(initialSelectedFilterOptions);
    const [isMenuOpen, setIsMenuOpen] = useState(false);
    const filterOptions = Object.keys(FormResponsesFilter).map((filterValue) => ({
        key: filterValue,
        label: (
            <FilterOption
                filterOption={{ field: FORM_RESPONSES_FILTER, value: filterValue }}
                selectedFilterOptions={selectedFilterOptions}
                setSelectedFilterOptions={setSelectedFilterOptions}
            />
        ),
        style: { padding: 0 },
    }));

    function updateFilters() {
        setFormResponsesFilters(selectedFilterOptions.map((option) => option.value as FormResponsesFilter));
        setIsMenuOpen(false);
    }

    function updateIsMenuOpen(isOpen: boolean) {
        setIsMenuOpen(isOpen);
        // set filters to default every time you open or close the menu without saving
        setSelectedFilterOptions(initialSelectedFilterOptions || []);
    }

    return (
        <SearchFilterView
            filterOptions={filterOptions}
            isMenuOpen={isMenuOpen}
            numActiveFilters={formResponsesFilters?.length || 0}
            filterIcon={<QuestionCircleOutlined />}
            displayName="Responses"
            searchQuery=""
            loading={false}
            updateIsMenuOpen={updateIsMenuOpen}
            setSearchQuery={() => {}}
            updateFilters={updateFilters}
            hideSearchBar
        />
    );
}
