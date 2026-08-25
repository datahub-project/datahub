import { Text } from '@components';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import ActorPill from '@app/sharedV2/owners/ActorPill';
import { SimpleSelect } from '@src/alchemy-components';

import { CorpGroup } from '@types';

const ALL_ACTORS_VALUE = 'All';

const StyledTag = styled.span`
    padding: 0px 7px 0px 7px;
    margin-right: 3px;
    display: flex;
    justify-content: start;
    align-items: center;
`;

const ActorWrapper = styled.div`
    margin-top: 2px;
    margin-right: 2px;
`;

export interface Props {
    groupSearchResults: any[] | undefined;
    groupsSelectUrns: string[];
    groupsSelectValues: CorpGroup[] | undefined;
    handleGroupSearch: (text: string) => void;
    onSelectGroupActor: (group: string) => void;
    onDeselectGroupActor: (group: string) => void;
    onClearAll?: () => void;
    renderSearchResult: (result: any) => React.ReactNode;
    onPreventMouseDown: (event: any) => void;
    placeholder: string;
    t: any;
}

export default function GroupsSelect({
    groupSearchResults,
    groupsSelectUrns,
    groupsSelectValues,
    handleGroupSearch,
    onSelectGroupActor,
    onDeselectGroupActor,
    onClearAll,
    renderSearchResult,
    onPreventMouseDown,
    placeholder,
    t,
}: Props) {
    const options = useMemo(() => {
        const searchOptions = [
            {
                value: ALL_ACTORS_VALUE,
                label: t('allGroups'),
                isAllOption: true,
            },
            ...(groupSearchResults?.map((result) => ({
                value: result.entity.urn,
                label: result.entity.urn,
                result,
            })) || []),
        ];
        // Selected groups must always exist in the option list — the select only renders
        // chips for values with a matching option, so an existing policy's groups would
        // otherwise be invisible (and unremovable) when absent from current search results.
        const optionUrns = new Set(searchOptions.map((option) => option.value));
        const selectedOnlyOptions =
            groupsSelectValues
                ?.filter((group) => groupsSelectUrns.includes(group.urn) && !optionUrns.has(group.urn))
                .map((group) => ({
                    value: group.urn,
                    label: group.properties?.displayName || group.name || group.urn,
                    selectedGroup: group,
                })) || [];
        return [...searchOptions, ...selectedOnlyOptions];
    }, [groupSearchResults, groupsSelectValues, groupsSelectUrns, t]);

    const handleUpdate = useCallback(
        (next: string[]) => {
            const current = new Set(groupsSelectUrns);
            const updated = new Set(next);

            // Find added items
            updated.forEach((item) => {
                if (!current.has(item)) {
                    onSelectGroupActor(item);
                }
            });

            // Find removed items
            current.forEach((item) => {
                if (!updated.has(item)) {
                    onDeselectGroupActor(item);
                }
            });
        },
        [groupsSelectUrns, onSelectGroupActor, onDeselectGroupActor],
    );

    const renderOption = useCallback(
        (option: any) => {
            if (option.isAllOption) {
                return <Text size="sm">{t('allGroups')}</Text>;
            }
            if (!option.result) {
                // Selected-only option (not in current search results) — render by label.
                return <Text size="sm">{option.label}</Text>;
            }
            return renderSearchResult(option.result);
        },
        [renderSearchResult, t],
    );

    const renderSelectedValue = useCallback(
        (option: any) => {
            if (option.value === ALL_ACTORS_VALUE) {
                return (
                    <StyledTag key={option.value} onMouseDown={onPreventMouseDown}>
                        {t('allGroups')}
                    </StyledTag>
                );
            }

            const selectedItem: CorpGroup | undefined = groupsSelectValues?.find((g) => g?.urn === option.value);
            return (
                <ActorWrapper key={option.value} onMouseDown={onPreventMouseDown}>
                    <ActorPill
                        actor={selectedItem}
                        isProposed={false}
                        hideLink
                        onClose={() => onDeselectGroupActor(option.value)}
                    />
                </ActorWrapper>
            );
        },
        [groupsSelectValues, onDeselectGroupActor, onPreventMouseDown, t],
    );

    return (
        <SimpleSelect
            isMultiSelect
            showSearch
            values={groupsSelectUrns}
            onUpdate={handleUpdate}
            onClear={onClearAll}
            options={options}
            combinedSelectedAndSearchOptions={options}
            onSearchChange={handleGroupSearch}
            renderCustomOptionText={renderOption}
            renderCustomSelectedValue={renderSelectedValue}
            selectLabelProps={{ variant: 'custom' }}
            filterResultsByQuery={false}
            placeholder={placeholder}
            width="full"
            dataTestId="groups"
            sortSelectedFirst={false}
        />
    );
}
