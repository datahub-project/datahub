import { Text } from '@components';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import ActorPill from '@app/sharedV2/owners/ActorPill';
import { SimpleSelect } from '@src/alchemy-components';

import { CorpUser } from '@types';

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
    userSearchResults: any[] | undefined;
    usersSelectUrns: string[];
    usersSelectValues: CorpUser[] | undefined;
    handleUserSearch: (text: string) => void;
    onSelectUserActor: (user: string) => void;
    onDeselectUserActor: (user: string) => void;
    onClearAll?: () => void;
    renderSearchResult: (result: any) => React.ReactNode;
    onPreventMouseDown: (event: any) => void;
    placeholder: string;
    t: any;
}

export default function UsersSelect({
    userSearchResults,
    usersSelectUrns,
    usersSelectValues,
    handleUserSearch,
    onSelectUserActor,
    onDeselectUserActor,
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
                label: t('allUsers'),
                isAllOption: true,
            },
            ...(userSearchResults?.map((result) => ({
                value: result.entity.urn,
                label: result.entity.urn,
                result,
            })) || []),
        ];
        // Selected users must always exist in the option list — the select only renders
        // chips for values with a matching option, so an existing policy's users would
        // otherwise be invisible (and unremovable) when absent from current search results.
        const optionUrns = new Set(searchOptions.map((option) => option.value));
        const selectedOnlyOptions =
            usersSelectValues
                ?.filter((user) => usersSelectUrns.includes(user.urn) && !optionUrns.has(user.urn))
                .map((user) => ({
                    value: user.urn,
                    label: user.properties?.displayName || user.username || user.urn,
                    selectedUser: user,
                })) || [];
        return [...searchOptions, ...selectedOnlyOptions];
    }, [userSearchResults, usersSelectValues, usersSelectUrns, t]);

    const handleUpdate = useCallback(
        (next: string[]) => {
            const current = new Set(usersSelectUrns);
            const updated = new Set(next);

            // Find added items
            updated.forEach((item) => {
                if (!current.has(item)) {
                    onSelectUserActor(item);
                }
            });

            // Find removed items
            current.forEach((item) => {
                if (!updated.has(item)) {
                    onDeselectUserActor(item);
                }
            });
        },
        [usersSelectUrns, onSelectUserActor, onDeselectUserActor],
    );

    const renderOption = useCallback(
        (option: any) => {
            if (option.isAllOption) {
                return <Text size="sm">{t('allUsers')}</Text>;
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
                        {t('allUsers')}
                    </StyledTag>
                );
            }

            const selectedItem: CorpUser | undefined = usersSelectValues?.find((u) => u?.urn === option.value);
            return (
                <ActorWrapper key={option.value} onMouseDown={onPreventMouseDown}>
                    <ActorPill
                        actor={selectedItem}
                        isProposed={false}
                        hideLink
                        onClose={() => onDeselectUserActor(option.value)}
                    />
                </ActorWrapper>
            );
        },
        [usersSelectValues, onDeselectUserActor, onPreventMouseDown, t],
    );

    return (
        <SimpleSelect
            isMultiSelect
            showSearch
            values={usersSelectUrns}
            onUpdate={handleUpdate}
            onClear={onClearAll}
            options={options}
            combinedSelectedAndSearchOptions={options}
            onSearchChange={handleUserSearch}
            renderCustomOptionText={renderOption}
            renderCustomSelectedValue={renderSelectedValue}
            selectLabelProps={{ variant: 'custom' }}
            filterResultsByQuery={false}
            placeholder={placeholder}
            width="full"
            dataTestId="users"
            sortSelectedFirst={false}
        />
    );
}
