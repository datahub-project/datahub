import { Avatar, Switch, Text } from '@components';
import { Form } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import GroupsSelect from '@app/permissions/policy/PolicyActorForm/GroupsSelect';
import OwnershipTypesSelect from '@app/permissions/policy/PolicyActorForm/OwnershipTypesSelect';
import UsersSelect from '@app/permissions/policy/PolicyActorForm/UsersSelect';
import useDebouncedCallback from '@app/shared/hooks/useDebouncedCallback';
import { useGetRecommendations } from '@app/shared/recommendation';
import { addUserFiltersToMultiEntitySearchInput } from '@app/shared/userSearchUtils';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleLazyQuery } from '@graphql/search.generated';
import { ActorFilter, CorpGroup, CorpUser, EntityType, PolicyType, SearchResult } from '@types';

type Props = {
    policyType: PolicyType;
    actors: ActorFilter;
    setActors: (actors: ActorFilter) => void;
};

const SearchResultContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 2px;
`;

const ActorForm = styled(Form)`
    margin: 12px;
    margin-top: 36px;
    margin-bottom: 40px;
`;

const ActorFormHeader = styled.div`
    margin-bottom: 16px;
`;

const SearchResultContent = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 3px;
`;

const SwitchWrapper = styled.div`
    margin-top: 8px;
    margin-bottom: 8px;
`;

const OwnershipWrapper = styled.div`
    margin-top: 12px;
`;

/**
 * Component used to construct the "actors" portion of a DataHub
 * access Policy by populating an ActorFilter object.
 */
export default function PolicyActorForm({ policyType, actors, setActors }: Props) {
    const { t } = useTranslation('settings.permissions');
    const entityRegistry = useEntityRegistry();

    // Track search input state
    const [userSearchInput, setUserSearchInput] = useState('');
    const [groupSearchInput, setGroupSearchInput] = useState('');

    // Search for actors while building policy.
    const [userSearch, { data: userSearchData }] = useGetSearchResultsForMultipleLazyQuery();
    const [groupSearch, { data: groupSearchData }] = useGetSearchResultsForMultipleLazyQuery();

    // Get recommended users and groups
    const { recommendedData: recommendedUsers } = useGetRecommendations([EntityType.CorpUser]);
    const { recommendedData: recommendedGroups } = useGetRecommendations([EntityType.CorpGroup]);

    const { data: ownershipData } = useOwnershipTypes();
    const ownershipTypes =
        ownershipData?.listOwnershipTypes?.ownershipTypes?.filter((type) => type.urn !== 'urn:li:ownershipType:none') ||
        [];
    const ownershipTypesMap = Object.fromEntries(ownershipTypes.map((type) => [type.urn, type.info?.name]));
    // Toggle the "Owners" switch
    const onToggleAppliesToOwners = () => {
        const newValue = !actors.resourceOwners;
        setActors({
            ...actors,
            resourceOwners: newValue,
            resourceOwnersTypes: newValue ? actors.resourceOwnersTypes : null,
        });
    };

    const onSelectOwnershipTypeActor = (newType: string) => {
        const newResourceOwnersTypes: Maybe<string[]> = [...(actors.resourceOwnersTypes || []), newType];
        setActors({
            ...actors,
            resourceOwnersTypes: newResourceOwnersTypes,
        });
    };

    const onDeselectOwnershipTypeActor = (type: string) => {
        const newResourceOwnersTypes: Maybe<string[]> = actors.resourceOwnersTypes?.filter((u: string) => u !== type);
        setActors({
            ...actors,
            resourceOwnersTypes: newResourceOwnersTypes?.length ? newResourceOwnersTypes : null,
        });
    };

    // User and group dropdown search results!
    // Show recommendations when no search input, otherwise show search results
    const userSearchResults: Array<{ entity: CorpUser }> | undefined =
        !userSearchInput || userSearchInput.length === 0
            ? recommendedUsers?.map((user) => ({ entity: user as CorpUser }))
            : (userSearchData?.searchAcrossEntities?.searchResults as Array<{ entity: CorpUser }> | undefined);
    const groupSearchResults: Array<{ entity: CorpGroup }> | undefined =
        !groupSearchInput || groupSearchInput.length === 0
            ? recommendedGroups?.map((group) => ({ entity: group as CorpGroup }))
            : (groupSearchData?.searchAcrossEntities?.searchResults as Array<{ entity: CorpGroup }> | undefined);

    // When a user search result is selected, add the urn to the ActorFilter
    const onSelectUserActor = (newUser: string) => {
        if (newUser === 'All') {
            setActors({
                ...actors,
                allUsers: true,
            });
        } else {
            // If "All Users" was previously selected, clear it and start fresh
            const existingUsers = actors.allUsers ? [] : actors.users || [];
            // Avoid duplicates by checking if user already exists
            if (existingUsers.includes(newUser)) {
                return;
            }
            const newUserActors = [...existingUsers, newUser];

            // Find the selected user entity from search results and add it to resolved users
            const selectedUserEntity = userSearchResults?.find((result) => result.entity.urn === newUser)
                ?.entity as CorpUser;
            const existingResolvedUsers = actors.allUsers ? [] : actors.resolvedUsers || [];
            const newResolvedUsers = selectedUserEntity
                ? [...existingResolvedUsers, selectedUserEntity]
                : existingResolvedUsers;

            setActors({
                ...actors,
                allUsers: false,
                users: newUserActors,
                resolvedUsers: newResolvedUsers,
            });
        }
    };

    // When a user search result is deselected, remove the urn from the ActorFilter
    const onDeselectUserActor = (user: string) => {
        if (user === 'All') {
            setActors({
                ...actors,
                allUsers: false,
            });
        } else {
            setActors({
                ...actors,
                users: actors.users?.filter((u) => u !== user),
                resolvedUsers: actors.resolvedUsers?.filter((u) => u.urn !== user),
            });
        }
    };

    // When a group search result is selected, add the urn to the ActorFilter
    const onSelectGroupActor = (newGroup: string) => {
        if (newGroup === 'All') {
            setActors({
                ...actors,
                allGroups: true,
            });
        } else {
            // If "All Groups" was previously selected, clear it and start fresh
            const existingGroups = actors.allGroups ? [] : actors.groups || [];
            // Avoid duplicates by checking if group already exists
            if (existingGroups.includes(newGroup)) {
                return;
            }
            const newGroupActors = [...existingGroups, newGroup];

            // Find the selected group entity from search results and add it to resolved groups
            const selectedGroupEntity = groupSearchResults?.find((result) => result.entity.urn === newGroup)
                ?.entity as CorpGroup;
            const existingResolvedGroups = actors.allGroups ? [] : actors.resolvedGroups || [];
            const newResolvedGroups = selectedGroupEntity
                ? [...existingResolvedGroups, selectedGroupEntity]
                : existingResolvedGroups;

            setActors({
                ...actors,
                allGroups: false,
                groups: newGroupActors,
                resolvedGroups: newResolvedGroups,
            });
        }
    };

    // When a group search result is deselected, remove the urn from the ActorFilter
    const onDeselectGroupActor = (group: string) => {
        if (group === 'All') {
            setActors({
                ...actors,
                allGroups: false,
            });
        } else {
            const newGroupActors = actors.groups?.filter((g) => g !== group);
            setActors({
                ...actors,
                groups: newGroupActors,
            });
        }
    };

    // Clear all users
    const onClearAllUsers = () => {
        setActors({
            ...actors,
            allUsers: false,
            users: undefined,
            resolvedUsers: undefined,
        });
    };

    // Clear all groups
    const onClearAllGroups = () => {
        setActors({
            ...actors,
            allGroups: false,
            groups: undefined,
            resolvedGroups: undefined,
        });
    };

    // Clear all ownership types
    const onClearAllOwnershipTypes = () => {
        setActors({
            ...actors,
            resourceOwnersTypes: undefined,
        });
    };

    // Invokes the search API as the user types
    const handleSearch = (type: EntityType, text: string, searchQuery: any) => {
        const input = addUserFiltersToMultiEntitySearchInput(
            {
                types: [type],
                query: text,
                start: 0,
                count: 10,
            },
            [type],
        );

        searchQuery({
            variables: {
                input,
            },
        });
    };

    // Users and groups get their own debouncer so typing in one select can't cancel
    // the other's pending search. Only the network call is debounced — the input state
    // updates synchronously so the UI tracks what the user types.
    const debouncedUserSearch = useDebouncedCallback((text: string) => {
        handleSearch(EntityType.CorpUser, text, userSearch);
    });
    const debouncedGroupSearch = useDebouncedCallback((text: string) => {
        handleSearch(EntityType.CorpGroup, text, groupSearch);
    });

    // Invokes the user search API as the user types
    const handleUserSearch = (text: string) => {
        setUserSearchInput(text);
        debouncedUserSearch(text);
    };

    // Invokes the group search API as the user types
    const handleGroupSearch = (text: string) => {
        setGroupSearchInput(text);
        debouncedGroupSearch(text);
    };

    // Renders a search result in the select dropdown.
    const renderSearchResult = (result: SearchResult) => {
        const avatarUrl =
            result.entity.type === EntityType.CorpUser
                ? (result.entity as CorpUser).editableProperties?.pictureLink || undefined
                : undefined;
        const displayName = entityRegistry.getDisplayName(result.entity.type, result.entity);
        return (
            <SearchResultContainer>
                <SearchResultContent>
                    <Avatar
                        name={displayName}
                        imageUrl={avatarUrl}
                        type={result.entity.type === EntityType.CorpGroup ? AvatarType.group : AvatarType.user}
                    />
                    <Text color="gray" size="sm">
                        {displayName}
                    </Text>
                </SearchResultContent>
            </SearchResultContainer>
        );
    };

    // Whether to show "owners" switch.
    const showAppliesToOwners = policyType === PolicyType.Metadata;

    // Select dropdown values.
    const usersSelectUrns = actors.allUsers ? ['All'] : actors.resolvedUsers?.map((u) => u.urn) || [];
    const groupsSelectUrns = actors.allGroups ? ['All'] : actors.groups || [];
    const ownershipTypesSelectValue = actors.resourceOwnersTypes || [];
    const usersSelectValues = actors.resolvedUsers?.filter((u) => usersSelectUrns.includes(u.urn)) || [];
    const groupsSelectValues = actors.resolvedGroups?.filter((g) => groupsSelectUrns.includes(g.urn)) || [];

    const onPreventMouseDown = (event) => {
        event.preventDefault();
        event.stopPropagation();
    };

    return (
        <ActorForm layout="vertical">
            <ActorFormHeader>
                <Text size="lg">{t('actorForm.title')}</Text>
                <Text color="textSecondary">{t('actorForm.description')}</Text>
            </ActorFormHeader>
            {showAppliesToOwners && (
                <Form.Item label={<Text>{t('actorForm.ownersLabel')}</Text>} labelAlign="right">
                    <Text color="textSecondary">{t('actorForm.ownersDescription')}</Text>
                    <SwitchWrapper>
                        <Switch
                            label=""
                            labelPosition="right"
                            isChecked={actors.resourceOwners}
                            onChange={onToggleAppliesToOwners}
                        />
                    </SwitchWrapper>
                    {actors.resourceOwners && (
                        <OwnershipWrapper>
                            <Text color="textSecondary">{t('actorForm.ownershipTypesDescription')}</Text>
                            <OwnershipTypesSelect
                                ownershipTypes={ownershipTypes}
                                ownershipTypesSelectValue={ownershipTypesSelectValue}
                                ownershipTypesMap={ownershipTypesMap}
                                onSelectOwnershipTypeActor={onSelectOwnershipTypeActor}
                                onDeselectOwnershipTypeActor={onDeselectOwnershipTypeActor}
                                onClearAll={onClearAllOwnershipTypes}
                                onPreventMouseDown={onPreventMouseDown}
                                placeholder={t('actorForm.ownershipTypesPlaceholder')}
                            />
                        </OwnershipWrapper>
                    )}
                </Form.Item>
            )}
            <Form.Item label={<Text>{t('actorForm.usersLabel')}</Text>}>
                <Text color="textSecondary">{t('actorForm.usersDescription')}</Text>
                <UsersSelect
                    userSearchResults={userSearchResults}
                    usersSelectUrns={usersSelectUrns}
                    usersSelectValues={usersSelectValues}
                    handleUserSearch={handleUserSearch}
                    onSelectUserActor={onSelectUserActor}
                    onDeselectUserActor={onDeselectUserActor}
                    onClearAll={onClearAllUsers}
                    renderSearchResult={renderSearchResult}
                    onPreventMouseDown={onPreventMouseDown}
                    placeholder={t('actorForm.usersPlaceholder')}
                    t={t}
                />
            </Form.Item>
            <Form.Item label={<Text>{t('groupsLabel')}</Text>}>
                <Text color="textSecondary">{t('actorForm.groupsDescription')}</Text>
                <GroupsSelect
                    groupSearchResults={groupSearchResults}
                    groupsSelectUrns={groupsSelectUrns}
                    groupsSelectValues={groupsSelectValues}
                    handleGroupSearch={handleGroupSearch}
                    onSelectGroupActor={onSelectGroupActor}
                    onDeselectGroupActor={onDeselectGroupActor}
                    onClearAll={onClearAllGroups}
                    renderSearchResult={renderSearchResult}
                    onPreventMouseDown={onPreventMouseDown}
                    placeholder={t('actorForm.groupsPlaceholder')}
                    t={t}
                />
            </Form.Item>
        </ActorForm>
    );
}
