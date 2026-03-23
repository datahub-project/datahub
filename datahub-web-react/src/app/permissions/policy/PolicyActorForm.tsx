import { Avatar, Text } from '@components';
import { Form, Select, Switch, Tag, Typography } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import React, { useState } from 'react';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import ActorPill from '@app/sharedV2/owners/ActorPill';
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
    margin-bottom: 28px;
`;

const SearchResultContent = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 3px;
`;

const ActorWrapper = styled.div`
    margin-top: 2px;
    margin-right: 2px;
`;

const OwnershipWrapper = styled.div`
    margin-top: 12px;
`;

const StyledTag = styled(Tag)`
    padding: 0px 7px 0px 7px;
    margin-right: 3px;
    display: flex;
    justify-content: start;
    align-items: center;
`;

const ExclusionToggle = styled.button`
    background: none;
    border: none;
    padding: 0;
    margin-top: 10px;
    display: flex;
    align-items: center;
    gap: 4px;
    cursor: pointer;
    color: ${(props) => props.theme.styles?.['primary-color'] || '#1890ff'};
    font-size: 12px;

    &:hover {
        opacity: 0.75;
    }
`;

const ExclusionPanel = styled.div`
    margin-top: 10px;
    padding: 12px 14px;
    background: ${(props) => props.theme.styles?.['background-color-secondary'] || '#fafafa'};
    border: 1px solid ${(props) => props.theme.styles?.['border-color-base'] || '#e8e8e8'};
    border-radius: 6px;
`;

const ExclusionLabel = styled(Typography.Text)`
    display: block;
    font-size: 12px;
    margin-bottom: 8px;
    color: ${(props) => props.theme.styles?.['text-color-secondary'] || '#666'};
`;

/**
 * Component used to construct the "actors" portion of a DataHub
 * access Policy by populating an ActorFilter object.
 */
export default function PolicyActorForm({ policyType, actors, setActors }: Props) {
    const entityRegistry = useEntityRegistry();

    // Start expanded if the policy already has exclusions (e.g. editing an existing policy)
    const [showUserExclusions, setShowUserExclusions] = useState(() => !!actors.excludedUsers?.length);
    const [showGroupExclusions, setShowGroupExclusions] = useState(() => !!actors.excludedGroups?.length);

    const [userSearch, { data: userSearchData }] = useGetSearchResultsForMultipleLazyQuery();
    const [groupSearch, { data: groupSearchData }] = useGetSearchResultsForMultipleLazyQuery();
    const { data: ownershipData } = useOwnershipTypes();
    const ownershipTypes =
        ownershipData?.listOwnershipTypes?.ownershipTypes?.filter((type) => type.urn !== 'urn:li:ownershipType:none') ||
        [];
    const ownershipTypesMap = Object.fromEntries(ownershipTypes.map((type) => [type.urn, type.info?.name]));

    const onToggleAppliesToOwners = (value: boolean) => {
        setActors({
            ...actors,
            resourceOwners: value,
            resourceOwnersTypes: value ? actors.resourceOwnersTypes : null,
        });
    };

    const onSelectOwnershipTypeActor = (newType: string) => {
        const newResourceOwnersTypes: Maybe<string[]> = [...(actors.resourceOwnersTypes || []), newType];
        setActors({ ...actors, resourceOwnersTypes: newResourceOwnersTypes });
    };

    const onDeselectOwnershipTypeActor = (type: string) => {
        const newResourceOwnersTypes: Maybe<string[]> = actors.resourceOwnersTypes?.filter((u: string) => u !== type);
        setActors({
            ...actors,
            resourceOwnersTypes: newResourceOwnersTypes?.length ? newResourceOwnersTypes : null,
        });
    };

    const userSearchResults = userSearchData?.searchAcrossEntities?.searchResults;
    const groupSearchResults = groupSearchData?.searchAcrossEntities?.searchResults;

    const onSelectUserActor = (newUser: string) => {
        if (newUser === 'All') {
            setActors({ ...actors, allUsers: true });
        } else {
            const selectedUserEntity = userSearchResults?.find((result) => result.entity.urn === newUser)
                ?.entity as CorpUser;
            const newResolvedUsers = selectedUserEntity
                ? [...(actors.resolvedUsers || []), selectedUserEntity]
                : actors.resolvedUsers;
            setActors({ ...actors, users: [...(actors.users || []), newUser], resolvedUsers: newResolvedUsers });
        }
    };

    const onDeselectUserActor = (user: string) => {
        if (user === 'All') {
            setActors({ ...actors, allUsers: false });
        } else {
            setActors({ ...actors, users: actors.users?.filter((u) => u !== user) });
        }
    };

    const onSelectGroupActor = (newGroup: string) => {
        if (newGroup === 'All') {
            setActors({ ...actors, allGroups: true });
        } else {
            const selectedGroupEntity = groupSearchResults?.find((result) => result.entity.urn === newGroup)
                ?.entity as CorpGroup;
            const newResolvedGroups = selectedGroupEntity
                ? [...(actors.resolvedGroups || []), selectedGroupEntity]
                : actors.resolvedGroups;
            setActors({
                ...actors,
                groups: [...(actors.groups || []), newGroup],
                resolvedGroups: newResolvedGroups,
            });
        }
    };

    const onDeselectGroupActor = (group: string) => {
        if (group === 'All') {
            setActors({ ...actors, allGroups: false });
        } else {
            setActors({ ...actors, groups: actors.groups?.filter((g) => g !== group) });
        }
    };

    const onSelectExcludedUserActor = (user: string) => {
        const selectedUserEntity = userSearchResults?.find((result) => result.entity.urn === user)?.entity as CorpUser;
        const newResolvedExcludedUsers = selectedUserEntity
            ? [...(actors.resolvedExcludedUsers || []), selectedUserEntity]
            : actors.resolvedExcludedUsers;
        setActors({
            ...actors,
            excludedUsers: [...(actors.excludedUsers || []), user],
            resolvedExcludedUsers: newResolvedExcludedUsers,
        });
    };

    const onDeselectExcludedUserActor = (user: string) => {
        setActors({ ...actors, excludedUsers: actors.excludedUsers?.filter((u) => u !== user) });
    };

    const onSelectExcludedGroupActor = (group: string) => {
        const selectedGroupEntity = groupSearchResults?.find((result) => result.entity.urn === group)
            ?.entity as CorpGroup;
        const newResolvedExcludedGroups = selectedGroupEntity
            ? [...(actors.resolvedExcludedGroups || []), selectedGroupEntity]
            : actors.resolvedExcludedGroups;
        setActors({
            ...actors,
            excludedGroups: [...(actors.excludedGroups || []), group],
            resolvedExcludedGroups: newResolvedExcludedGroups,
        });
    };

    const onDeselectExcludedGroupActor = (group: string) => {
        setActors({ ...actors, excludedGroups: actors.excludedGroups?.filter((g) => g !== group) });
    };

    const handleSearch = (type: EntityType, text: string, searchQuery: any) => {
        searchQuery({
            variables: { input: { types: [type], query: text, start: 0, count: 10 } },
        });
    };

    const handleUserSearch = (text: string) => handleSearch(EntityType.CorpUser, text, userSearch);
    const handleGroupSearch = (text: string) => handleSearch(EntityType.CorpGroup, text, groupSearch);

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

    const showAppliesToOwners = policyType === PolicyType.Metadata;

    const usersSelectUrns = actors.allUsers ? ['All'] : actors.users || [];
    const groupsSelectUrns = actors.allGroups ? ['All'] : actors.groups || [];
    const ownershipTypesSelectValue = actors.resourceOwnersTypes || [];
    const usersSelectValues = actors.resolvedUsers?.filter((u) => usersSelectUrns.includes(u.urn)) || [];
    const groupsSelectValues = actors.resolvedGroups?.filter((g) => groupsSelectUrns.includes(g.urn)) || [];
    const excludedUsersSelectUrns = actors.excludedUsers || [];
    const excludedGroupsSelectUrns = actors.excludedGroups || [];
    const excludedUsersSelectValues =
        actors.resolvedExcludedUsers?.filter((u) => excludedUsersSelectUrns.includes(u.urn)) || [];
    const excludedGroupsSelectValues =
        actors.resolvedExcludedGroups?.filter((g) => excludedGroupsSelectUrns.includes(g.urn)) || [];

    const onPreventMouseDown = (event) => {
        event.preventDefault();
        event.stopPropagation();
    };

    return (
        <ActorForm layout="vertical">
            <ActorFormHeader>
                <Typography.Title level={4}>Applies to</Typography.Title>
                <Typography.Paragraph>Select the users & groups that this policy should apply to.</Typography.Paragraph>
            </ActorFormHeader>
            {showAppliesToOwners && (
                <Form.Item label={<Typography.Text strong>Owners</Typography.Text>} labelAlign="right">
                    <Typography.Paragraph>
                        Whether this policy should be apply to owners of the Metadata asset. If true, those who are
                        marked as owners of a Metadata Asset, either directly or indirectly via a Group, will have the
                        selected privileges.
                    </Typography.Paragraph>
                    <Switch size="small" checked={actors.resourceOwners} onChange={onToggleAppliesToOwners} />
                    {actors.resourceOwners && (
                        <OwnershipWrapper>
                            <Typography.Paragraph>
                                List of types of ownership which will be used to match owners. If empty, the policies
                                will applied to any type of ownership.
                            </Typography.Paragraph>
                            <Select
                                value={ownershipTypesSelectValue}
                                mode="multiple"
                                placeholder="Ownership types"
                                onSelect={(asset: any) => onSelectOwnershipTypeActor(asset)}
                                onDeselect={(asset: any) => onDeselectOwnershipTypeActor(asset)}
                                tagRender={(tagProps) => {
                                    return (
                                        <Tag closable={tagProps.closable} onClose={tagProps.onClose}>
                                            {ownershipTypesMap[tagProps.value.toString()]}
                                        </Tag>
                                    );
                                }}
                            >
                                {ownershipTypes.map((resOwnershipType) => {
                                    return (
                                        <Select.Option value={resOwnershipType.urn}>
                                            {resOwnershipType?.info?.name}
                                        </Select.Option>
                                    );
                                })}
                            </Select>
                        </OwnershipWrapper>
                    )}
                </Form.Item>
            )}
            <Form.Item label={<Typography.Text strong>Users & Service Accounts</Typography.Text>}>
                <Typography.Paragraph>
                    Search for specific users or service accounts that this policy should apply to, or select `All
                    Users` to apply it to all users.
                </Typography.Paragraph>
                <Select
                    data-testid="users"
                    value={usersSelectUrns}
                    mode="multiple"
                    filterOption={false}
                    placeholder="Search for users..."
                    onSelect={(asset: any) => onSelectUserActor(asset)}
                    onDeselect={(asset: any) => onDeselectUserActor(asset)}
                    onSearch={handleUserSearch}
                    tagRender={(tagProps) => {
                        const { closable, onClose, value } = tagProps;
                        const handleClose = (event) => {
                            onPreventMouseDown(event);
                            onClose();
                        };
                        if (value === 'All') {
                            return (
                                <StyledTag closable={closable} onClose={handleClose} onMouseDown={onPreventMouseDown}>
                                    All Users
                                </StyledTag>
                            );
                        }
                        const selectedItem: CorpUser | undefined = usersSelectValues?.find((u) => u?.urn === value);
                        return (
                            <ActorWrapper onMouseDown={onPreventMouseDown}>
                                <ActorPill actor={selectedItem} isProposed={false} hideLink onClose={handleClose} />
                            </ActorWrapper>
                        );
                    }}
                >
                    {userSearchResults?.map((result) => (
                        <Select.Option value={result.entity.urn}>{renderSearchResult(result)}</Select.Option>
                    ))}
                    <Select.Option value="All">All Users</Select.Option>
                </Select>
                <ExclusionToggle
                    type="button"
                    onClick={() => setShowUserExclusions((prev) => !prev)}
                    aria-expanded={showUserExclusions}
                >
                    {showUserExclusions ? '▲' : '▼'} Exceptions
                </ExclusionToggle>
                {showUserExclusions && (
                    <ExclusionPanel>
                        <ExclusionLabel>
                            These users will be excluded from this policy even if they match the rules above.
                        </ExclusionLabel>
                        <Select
                            data-testid="excluded-users"
                            value={excludedUsersSelectUrns}
                            mode="multiple"
                            filterOption={false}
                            placeholder="Search for users to exclude..."
                            onSelect={(asset: any) => onSelectExcludedUserActor(asset)}
                            onDeselect={(asset: any) => onDeselectExcludedUserActor(asset)}
                            onSearch={handleUserSearch}
                            style={{ width: '100%' }}
                            tagRender={(tagProps) => {
                                const { onClose, value } = tagProps;
                                const handleClose = (event) => {
                                    onPreventMouseDown(event);
                                    onClose();
                                };
                                const selectedItem: CorpUser | undefined = excludedUsersSelectValues?.find(
                                    (u) => u?.urn === value,
                                );
                                return (
                                    <ActorWrapper onMouseDown={onPreventMouseDown}>
                                        <ActorPill
                                            actor={selectedItem}
                                            isProposed={false}
                                            hideLink
                                            onClose={handleClose}
                                        />
                                    </ActorWrapper>
                                );
                            }}
                        >
                            {userSearchResults?.map((result) => (
                                <Select.Option value={result.entity.urn}>{renderSearchResult(result)}</Select.Option>
                            ))}
                        </Select>
                    </ExclusionPanel>
                )}
            </Form.Item>
            <Form.Item label={<Typography.Text strong>Groups</Typography.Text>}>
                <Typography.Paragraph>
                    Search for specific groups that this policy should apply to, or select `All Groups` to apply it to
                    all groups.
                </Typography.Paragraph>
                <Select
                    data-testid="groups"
                    value={groupsSelectUrns}
                    mode="multiple"
                    placeholder="Search for groups..."
                    onSelect={(asset: any) => onSelectGroupActor(asset)}
                    onDeselect={(asset: any) => onDeselectGroupActor(asset)}
                    onSearch={handleGroupSearch}
                    filterOption={false}
                    tagRender={(tagProps) => {
                        const { closable, onClose, value } = tagProps;
                        const handleClose = (event) => {
                            onPreventMouseDown(event);
                            onClose();
                        };
                        if (value === 'All') {
                            return (
                                <StyledTag closable={closable} onClose={handleClose} onMouseDown={onPreventMouseDown}>
                                    All Groups
                                </StyledTag>
                            );
                        }
                        const selectedItem: CorpGroup | undefined = groupsSelectValues?.find((g) => g?.urn === value);
                        return (
                            <ActorWrapper onMouseDown={onPreventMouseDown}>
                                <ActorPill actor={selectedItem} isProposed={false} hideLink onClose={handleClose} />
                            </ActorWrapper>
                        );
                    }}
                >
                    {groupSearchResults?.map((result) => (
                        <Select.Option value={result.entity.urn}>{renderSearchResult(result)}</Select.Option>
                    ))}
                    <Select.Option value="All">All Groups</Select.Option>
                </Select>
                <ExclusionToggle
                    type="button"
                    onClick={() => setShowGroupExclusions((prev) => !prev)}
                    aria-expanded={showGroupExclusions}
                >
                    {showGroupExclusions ? '▲' : '▼'} Exceptions
                </ExclusionToggle>
                {showGroupExclusions && (
                    <ExclusionPanel>
                        <ExclusionLabel>
                            These groups will be excluded from this policy even if they match the rules above.
                        </ExclusionLabel>
                        <Select
                            data-testid="excluded-groups"
                            value={excludedGroupsSelectUrns}
                            mode="multiple"
                            placeholder="Search for groups to exclude..."
                            onSelect={(asset: any) => onSelectExcludedGroupActor(asset)}
                            onDeselect={(asset: any) => onDeselectExcludedGroupActor(asset)}
                            onSearch={handleGroupSearch}
                            filterOption={false}
                            style={{ width: '100%' }}
                            tagRender={(tagProps) => {
                                const { onClose, value } = tagProps;
                                const handleClose = (event) => {
                                    onPreventMouseDown(event);
                                    onClose();
                                };
                                const selectedItem: CorpGroup | undefined = excludedGroupsSelectValues?.find(
                                    (g) => g?.urn === value,
                                );
                                return (
                                    <ActorWrapper onMouseDown={onPreventMouseDown}>
                                        <ActorPill
                                            actor={selectedItem}
                                            isProposed={false}
                                            hideLink
                                            onClose={handleClose}
                                        />
                                    </ActorWrapper>
                                );
                            }}
                        >
                            {groupSearchResults?.map((result) => (
                                <Select.Option value={result.entity.urn}>{renderSearchResult(result)}</Select.Option>
                            ))}
                        </Select>
                    </ExclusionPanel>
                )}
            </Form.Item>
        </ActorForm>
    );
}
