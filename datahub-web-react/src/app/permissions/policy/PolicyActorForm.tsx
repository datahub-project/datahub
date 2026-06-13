import { Avatar, Text } from '@components';
import { Form, Radio, Select, Switch, Tag, Typography } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
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

type Condition = 'include' | 'exclude';

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

const SelectRow = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
`;

const ALL_ACTORS_VALUE = 'All';

const INCLUDE_CONDITION: Condition = 'include';
const EXCLUDE_CONDITION: Condition = 'exclude';

/**
 * Component used to construct the "actors" portion of a DataHub
 * access Policy by populating an ActorFilter object.
 */
export default function PolicyActorForm({ policyType, actors, setActors }: Props) {
    const { t } = useTranslation('settings.permissions');
    const entityRegistry = useEntityRegistry();

    const [userCondition, setUserCondition] = useState<Condition>(() =>
        actors.excludedUsers?.length ? 'exclude' : 'include',
    );
    const [groupCondition, setGroupCondition] = useState<Condition>(() =>
        actors.excludedGroups?.length ? 'exclude' : 'include',
    );
    const [ownershipTypeCondition, setOwnershipTypeCondition] = useState<Condition>(() =>
        actors.excludedResourceOwnersTypes?.length ? 'exclude' : 'include',
    );

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

    const onSelectExcludedOwnershipType = (type: string) => {
        setActors({ ...actors, excludedResourceOwnersTypes: [...(actors.excludedResourceOwnersTypes || []), type] });
    };

    const onDeselectExcludedOwnershipType = (type: string) => {
        const updated = actors.excludedResourceOwnersTypes?.filter((ot) => ot !== type);
        setActors({ ...actors, excludedResourceOwnersTypes: updated?.length ? updated : null });
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
        const updatedExcludedUsers = actors.excludedUsers?.filter((u) => u !== user);
        setActors({
            ...actors,
            excludedUsers: updatedExcludedUsers,
            resolvedExcludedUsers: actors.resolvedExcludedUsers?.filter((u) => u.urn !== user),
        });
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
        setActors({
            ...actors,
            excludedGroups: actors.excludedGroups?.filter((g) => g !== group),
            resolvedExcludedGroups: actors.resolvedExcludedGroups?.filter((g) => g.urn !== group),
        });
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

    const conditionSelect = (condition: Condition, onChange: (c: Condition) => void) => (
        <Radio.Group
            size="small"
            value={condition}
            buttonStyle="solid"
            onChange={(e) => onChange(e.target.value as Condition)}
        >
            <Radio.Button value={INCLUDE_CONDITION}>{t('includeOption')}</Radio.Button>
            <Radio.Button value={EXCLUDE_CONDITION}>{t('excludeOption')}</Radio.Button>
        </Radio.Group>
    );

    const showAppliesToOwners = policyType === PolicyType.Metadata;

    const includedUsersUrns = actors.allUsers ? ['All'] : actors.users || [];
    const usersSelectUrns = userCondition === 'include' ? includedUsersUrns : actors.excludedUsers || [];
    const includedGroupsUrns = actors.allGroups ? ['All'] : actors.groups || [];
    const groupsSelectUrns = groupCondition === 'include' ? includedGroupsUrns : actors.excludedGroups || [];
    const ownershipTypesSelectValue =
        ownershipTypeCondition === 'include'
            ? actors.resourceOwnersTypes || []
            : actors.excludedResourceOwnersTypes || [];

    const usersSelectValues = actors.resolvedUsers?.filter((u) => usersSelectUrns.includes(u.urn)) || [];
    const groupsSelectValues = actors.resolvedGroups?.filter((g) => groupsSelectUrns.includes(g.urn)) || [];
    const excludedUsersSelectValues =
        actors.resolvedExcludedUsers?.filter((u) => (actors.excludedUsers || []).includes(u.urn)) || [];
    const excludedGroupsSelectValues =
        actors.resolvedExcludedGroups?.filter((g) => (actors.excludedGroups || []).includes(g.urn)) || [];

    const onPreventMouseDown = (event) => {
        event.preventDefault();
        event.stopPropagation();
    };

    const renderUserTag = (tagProps) => {
        const { closable, onClose, value } = tagProps;
        const handleClose = (event) => {
            onPreventMouseDown(event);
            onClose();
        };
        if (value === 'All') {
            return (
                <StyledTag closable={closable} onClose={handleClose} onMouseDown={onPreventMouseDown}>
                    {t('allUsers')}
                </StyledTag>
            );
        }
        const selectedItem: CorpUser | undefined =
            userCondition === 'include'
                ? usersSelectValues?.find((u) => u?.urn === value)
                : excludedUsersSelectValues?.find((u) => u?.urn === value);
        return (
            <ActorWrapper onMouseDown={onPreventMouseDown}>
                <ActorPill actor={selectedItem} isProposed={false} hideLink onClose={handleClose} />
            </ActorWrapper>
        );
    };

    const renderGroupTag = (tagProps) => {
        const { closable, onClose, value } = tagProps;
        const handleClose = (event) => {
            onPreventMouseDown(event);
            onClose();
        };
        if (value === 'All') {
            return (
                <StyledTag closable={closable} onClose={handleClose} onMouseDown={onPreventMouseDown}>
                    {t('allGroups')}
                </StyledTag>
            );
        }
        const selectedItem: CorpGroup | undefined =
            groupCondition === 'include'
                ? groupsSelectValues?.find((g) => g?.urn === value)
                : excludedGroupsSelectValues?.find((g) => g?.urn === value);
        return (
            <ActorWrapper onMouseDown={onPreventMouseDown}>
                <ActorPill actor={selectedItem} isProposed={false} hideLink onClose={handleClose} />
            </ActorWrapper>
        );
    };

    return (
        <ActorForm layout="vertical">
            <ActorFormHeader>
                <Typography.Title level={4}>{t('actorForm.title')}</Typography.Title>
                <Typography.Paragraph>{t('actorForm.description')}</Typography.Paragraph>
            </ActorFormHeader>
            {showAppliesToOwners && (
                <Form.Item
                    label={<Typography.Text strong>{t('actorForm.ownersLabel')}</Typography.Text>}
                    labelAlign="right"
                >
                    <Typography.Paragraph>{t('actorForm.ownersDescription')}</Typography.Paragraph>
                    <Switch size="small" checked={actors.resourceOwners} onChange={onToggleAppliesToOwners} />
                    {actors.resourceOwners && (
                        <OwnershipWrapper>
                            <Form.Item
                                label={<Typography.Text strong>{t('actorForm.ownershipTypesLabel')}</Typography.Text>}
                                style={{ marginBottom: 0 }}
                            >
                                <Typography.Paragraph>
                                    {ownershipTypeCondition === 'include'
                                        ? t('actorForm.ownershipTypesDescription')
                                        : t('actorForm.ownershipTypesExcludeDescription')}
                                </Typography.Paragraph>
                                <SelectRow>
                                    {conditionSelect(ownershipTypeCondition, setOwnershipTypeCondition)}
                                    <Select
                                        style={{ flex: 1 }}
                                        value={ownershipTypesSelectValue}
                                        mode="multiple"
                                        placeholder={
                                            ownershipTypeCondition === 'include'
                                                ? t('actorForm.ownershipTypesPlaceholder')
                                                : t('actorForm.ownershipTypesExcludePlaceholder')
                                        }
                                        onSelect={(asset: any) =>
                                            ownershipTypeCondition === 'include'
                                                ? onSelectOwnershipTypeActor(asset)
                                                : onSelectExcludedOwnershipType(asset)
                                        }
                                        onDeselect={(asset: any) =>
                                            ownershipTypeCondition === 'include'
                                                ? onDeselectOwnershipTypeActor(asset)
                                                : onDeselectExcludedOwnershipType(asset)
                                        }
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
                                </SelectRow>
                            </Form.Item>
                        </OwnershipWrapper>
                    )}
                </Form.Item>
            )}
            <Form.Item label={<Typography.Text strong>{t('actorForm.usersLabel')}</Typography.Text>}>
                <Typography.Paragraph>
                    {userCondition === 'include'
                        ? t('actorForm.usersDescription')
                        : t('actorForm.usersExcludeDescription')}
                </Typography.Paragraph>
                <SelectRow>
                    {conditionSelect(userCondition, setUserCondition)}
                    <Select
                        data-testid="users"
                        style={{ flex: 1 }}
                        value={usersSelectUrns}
                        mode="multiple"
                        filterOption={false}
                        placeholder={
                            userCondition === 'include'
                                ? t('actorForm.usersPlaceholder')
                                : t('actorForm.usersExcludePlaceholder')
                        }
                        onSelect={(asset: any) =>
                            userCondition === 'include' ? onSelectUserActor(asset) : onSelectExcludedUserActor(asset)
                        }
                        onDeselect={(asset: any) =>
                            userCondition === 'include'
                                ? onDeselectUserActor(asset)
                                : onDeselectExcludedUserActor(asset)
                        }
                        onSearch={handleUserSearch}
                        tagRender={renderUserTag}
                    >
                        {userSearchResults?.map((result) => (
                            <Select.Option value={result.entity.urn}>{renderSearchResult(result)}</Select.Option>
                        ))}
                        {userCondition === 'include' && (
                            <Select.Option value={ALL_ACTORS_VALUE}>{t('allUsers')}</Select.Option>
                        )}
                    </Select>
                </SelectRow>
            </Form.Item>
            <Form.Item label={<Typography.Text strong>{t('groupsLabel')}</Typography.Text>}>
                <Typography.Paragraph>
                    {groupCondition === 'include'
                        ? t('actorForm.groupsDescription')
                        : t('actorForm.groupsExcludeDescription')}
                </Typography.Paragraph>
                <SelectRow>
                    {conditionSelect(groupCondition, setGroupCondition)}
                    <Select
                        data-testid="groups"
                        style={{ flex: 1 }}
                        value={groupsSelectUrns}
                        mode="multiple"
                        placeholder={
                            groupCondition === 'include'
                                ? t('actorForm.groupsPlaceholder')
                                : t('actorForm.groupsExcludePlaceholder')
                        }
                        onSelect={(asset: any) =>
                            groupCondition === 'include' ? onSelectGroupActor(asset) : onSelectExcludedGroupActor(asset)
                        }
                        onDeselect={(asset: any) =>
                            groupCondition === 'include'
                                ? onDeselectGroupActor(asset)
                                : onDeselectExcludedGroupActor(asset)
                        }
                        onSearch={handleGroupSearch}
                        filterOption={false}
                        tagRender={renderGroupTag}
                    >
                        {groupSearchResults?.map((result) => (
                            <Select.Option value={result.entity.urn}>{renderSearchResult(result)}</Select.Option>
                        ))}
                        {groupCondition === 'include' && (
                            <Select.Option value={ALL_ACTORS_VALUE}>{t('allGroups')}</Select.Option>
                        )}
                    </Select>
                </SelectRow>
            </Form.Item>
        </ActorForm>
    );
}
