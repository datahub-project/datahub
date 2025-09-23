import { Text } from '@components';
import { Form, Select, Switch, Tag, Typography } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';

import { CustomAvatar } from '@app/shared/avatar';
import ActorPill from '@app/sharedV2/owners/ActorPill';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useListOwnershipTypesQuery } from '@graphql/ownership.generated';
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

/**
 * Component used to construct the "actors" portion of a DataHub
 * access Policy by populating an ActorFilter object.
 */
export default function PolicyActorForm({ policyType, actors, setActors }: Props) {
    const entityRegistry = useEntityRegistry();

    // Search for actors while building policy.
    const [userSearch, { data: userSearchData }] = useGetSearchResultsForMultipleLazyQuery();
    const [groupSearch, { data: groupSearchData }] = useGetSearchResultsForMultipleLazyQuery();
    const { data: ownershipData } = useListOwnershipTypesQuery({
        variables: {
            input: {},
        },
    });
    const ownershipTypes =
        ownershipData?.listOwnershipTypes?.ownershipTypes?.filter((type) => type.urn !== 'urn:li:ownershipType:none') ||
        [];
    const ownershipTypesMap = Object.fromEntries(ownershipTypes.map((type) => [type.urn, type.info?.name]));
    // Toggle the "Owners" switch
    const onToggleAppliesToOwners = (value: boolean) => {
        setActors({
            ...actors,
            resourceOwners: value,
            resourceOwnersTypes: value ? actors.resourceOwnersTypes : null,
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
    const userSearchResults = userSearchData?.searchAcrossEntities?.searchResults;
    const groupSearchResults = groupSearchData?.searchAcrossEntities?.searchResults;

    // When a user search result is selected, add the urn to the ActorFilter
    const onSelectUserActor = (newUser: string) => {
        if (newUser === 'All') {
            setActors({
                ...actors,
                allUsers: true,
            });
        } else {
            const newUserActors = [...(actors.users || []), newUser];

            // Find the selected user entity from search results and add it to resolved users
            const selectedUserEntity = userSearchResults?.find((result) => result.entity.urn === newUser)
                ?.entity as CorpUser;
            const newResolvedUsers = selectedUserEntity
                ? [...(actors.resolvedUsers || []), selectedUserEntity]
                : actors.resolvedUsers;

            setActors({
                ...actors,
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
            const newUserActors = actors.users?.filter((u) => u !== user);
            setActors({
                ...actors,
                users: newUserActors,
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
            const newGroupActors = [...(actors.groups || []), newGroup];

            // Find the selected group entity from search results and add it to resolved groups
            const selectedGroupEntity = groupSearchResults?.find((result) => result.entity.urn === newGroup)
                ?.entity as CorpGroup;
            const newResolvedGroups = selectedGroupEntity
                ? [...(actors.resolvedGroups || []), selectedGroupEntity]
                : actors.resolvedGroups;

            setActors({
                ...actors,
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

    // Invokes the search API as the user types
    const handleSearch = (type: EntityType, text: string, searchQuery: any) => {
        searchQuery({
            variables: {
                input: {
                    types: [type],
                    query: text,
                    start: 0,
                    count: 10,
                },
            },
        });
    };

    // Invokes the user search API as the user types
    const handleUserSearch = (text: string) => {
        return handleSearch(EntityType.CorpUser, text, userSearch);
    };

    // Invokes the group search API as the user types
    const handleGroupSearch = (text: string) => {
        return handleSearch(EntityType.CorpGroup, text, groupSearch);
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
                    <CustomAvatar size={20} name={displayName} photoUrl={avatarUrl} hideTooltip />
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
    const usersSelectUrns = actors.allUsers ? ['All'] : actors.users || [];
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
            <Form.Item label={<Typography.Text strong>Users</Typography.Text>}>
                <Typography.Paragraph>
                    Search for specific users that this policy should apply to, or select `All Users` to apply it to all
                    users.
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
            </Form.Item>
        </ActorForm>
    );
}
