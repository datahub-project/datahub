import React from 'react';
import { Form, Select, Switch, Tag, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { useEntityRegistry } from '../useEntityRegistry';
import { ActorFilter, EntityType, PolicyType, SearchResult } from '../../types.generated';
import { useGetSearchResultsLazyQuery } from '../../graphql/search.generated';

type Props = {
    policyType: string; // TODO Move to enum.
    actors: ActorFilter;
    setActors: (actors: ActorFilter) => void;
};

export default function PolicyActorForm({ policyType, actors, setActors }: Props) {
    const [userSearch, { data: userSearchData, loading: userSearchLoading }] = useGetSearchResultsLazyQuery();
    const [groupSearch, { data: groupSearchData, loading: groupSearchLoading }] = useGetSearchResultsLazyQuery();

    const entityRegistry = useEntityRegistry();

    const onToggleAppliesToOwners = (value: boolean) => {
        setActors({
            ...actors,
            resourceOwners: value,
        });
    };

    const onSelectUserActor = (newUser: string) => {
        if (newUser === 'All') {
            setActors({
                ...actors,
                allUsers: true,
            });
        } else {
            const newUserActors = [...(actors.users || []), newUser];
            setActors({
                ...actors,
                users: newUserActors,
            });
        }
    };

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

    const onSelectGroupActor = (newGroup: string) => {
        if (newGroup === 'All') {
            setActors({
                ...actors,
                allGroups: true,
            });
        } else {
            const newGroupActors = [...(actors.groups || []), newGroup];
            setActors({
                ...actors,
                groups: newGroupActors,
            });
        }
    };

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

    const handleSearch = (type: EntityType, text: string, searchQuery: any) => {
        if (text.length > 2) {
            searchQuery({
                variables: {
                    input: {
                        type,
                        query: text,
                        start: 0,
                        count: 10,
                    },
                },
            });
        }
    };

    const handleUserSearch = (event: any) => {
        const text = event.target.value as string;
        return handleSearch(EntityType.CorpUser, text, userSearch);
    };

    const handleGroupSearch = (event: any) => {
        const text = event.target.value as string;
        return handleSearch(EntityType.CorpGroup, text, groupSearch);
    };

    const renderSearchResult = (result: SearchResult) => {
        return (
            <div style={{ padding: 12, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                {entityRegistry.getDisplayName(result.entity.type, result.entity)}
                <Link
                    target="_blank"
                    rel="noopener noreferrer"
                    to={() => `/${entityRegistry.getPathName(result.entity.type)}/${result.entity.urn}`}
                >
                    View
                </Link>{' '}
            </div>
        );
    };

    const userSearchResults = userSearchData?.search?.searchResults;
    const groupSearchResults = groupSearchData?.search?.searchResults;
    const showAppliesToOwners = policyType === PolicyType.Metadata;

    const usersSelectValue = actors.allUsers ? ['All'] : actors.users || [];
    const groupsSelectValue = actors.allGroups ? ['All'] : actors.groups || [];

    return (
        <Form layout="vertical" initialValues={{}} style={{ margin: 12, marginTop: 36, marginBottom: 40 }}>
            <Typography.Title level={4}>Applies to</Typography.Title>
            <Typography.Paragraph>
                Select the users & groups that this policy should be applied to.
            </Typography.Paragraph>
            {showAppliesToOwners && (
                <Form.Item label={<Typography.Text strong>Owners</Typography.Text>} labelAlign="right">
                    <Typography.Paragraph>
                        Whether this policy should be apply to owners of the Metadata asset. If true, those who are
                        marked as owners of a Metadata Asset, either directly or indirectly via a Group, will have the
                        selected privileges.
                    </Typography.Paragraph>
                    <Switch size="small" checked={actors.resourceOwners} onChange={onToggleAppliesToOwners} />
                </Form.Item>
            )}
            <Form.Item label={<Typography.Text strong>Users</Typography.Text>}>
                <Typography.Paragraph>
                    Search for specific users that this policy should apply to, or select `All Users` to apply it to all
                    users.
                </Typography.Paragraph>
                <Select
                    value={usersSelectValue}
                    mode="multiple"
                    placeholder="Search for users..."
                    onSelect={(asset: any) => onSelectUserActor(asset)}
                    onDeselect={(asset: any) => onDeselectUserActor(asset)}
                    onInputKeyDown={handleUserSearch}
                    tagRender={(tagProps) => (
                        <Tag closable={tagProps.closable} onClose={tagProps.onClose}>
                            {tagProps.value}
                        </Tag>
                    )}
                >
                    {userSearchResults &&
                        userSearchResults.map((result) => (
                            <Select.Option value={result.entity.urn}>{renderSearchResult(result)}</Select.Option>
                        ))}
                    {userSearchLoading && <Select.Option value="loading">Searching...</Select.Option>}
                    <Select.Option value="All">All Users</Select.Option>
                </Select>
            </Form.Item>
            <Form.Item label={<Typography.Text strong>Groups</Typography.Text>}>
                <Typography.Paragraph>
                    Search for specific groups that this policy should apply to, or select `All Groups` to apply it to
                    all groups.
                </Typography.Paragraph>
                <Select
                    value={groupsSelectValue}
                    mode="multiple"
                    placeholder="Search for groups..."
                    onSelect={(asset: any) => onSelectGroupActor(asset)}
                    onDeselect={(asset: any) => onDeselectGroupActor(asset)}
                    onInputKeyDown={handleGroupSearch}
                    tagRender={(tagProps) => (
                        <Tag closable={tagProps.closable} onClose={tagProps.onClose}>
                            {tagProps.value}
                        </Tag>
                    )}
                >
                    {groupSearchResults &&
                        groupSearchResults.map((result) => (
                            <Select.Option value={result.entity.urn}>{renderSearchResult(result)}</Select.Option>
                        ))}
                    {groupSearchLoading && <Select.Option value="loading">Searching...</Select.Option>}
                    <Select.Option value="All">All Groups</Select.Option>
                </Select>
            </Form.Item>
        </Form>
    );
}
