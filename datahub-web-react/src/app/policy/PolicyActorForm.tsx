import React from 'react';
import { Form, Select, Switch, Tag, Typography } from 'antd';
import { useEntityRegistry } from '../useEntityRegistry';
import { EntityType, SearchResult } from '../../types.generated';
import { PreviewType } from '../entity/Entity';
import { useGetSearchResultsLazyQuery } from '../../graphql/search.generated';

type Props = {
    appliesToOwners: boolean;
    setAppliesToOwners: (newValue: boolean) => void;
    userUrns: Array<string>;
    setUserUrns: (newUsers: Array<string>) => void;
    groupUrns: Array<string>;
    setGroupUrns: (newGroups: Array<string>) => void;
};

export default function PolicyActorForm({
    appliesToOwners,
    setAppliesToOwners,
    userUrns,
    setUserUrns,
    groupUrns,
    setGroupUrns,
}: Props) {
    const [userSearch, { data: userSearchData, loading: userSearchLoading }] = useGetSearchResultsLazyQuery();
    const [groupSearch, { data: groupSearchData, loading: groupSearchLoading }] = useGetSearchResultsLazyQuery();

    const entityRegistry = useEntityRegistry();

    const onToggleAppliesToOwners = (value: boolean) => {
        setAppliesToOwners(value);
    };

    const onSelectUserActor = (newUser: string) => {
        const newUserActors = [...userUrns, newUser];
        setUserUrns(newUserActors as never[]);
    };

    const onDeselectUserActor = (user: string) => {
        const newUserActors = userUrns.filter((u) => u !== user);
        setUserUrns(newUserActors as never[]);
    };

    const onSelectGroupActor = (newGroup: string) => {
        const newGroupActors = [...groupUrns, newGroup];
        setGroupUrns(newGroupActors as never[]);
    };

    const onDeselectGroupActor = (group: string) => {
        const newGroupActors = groupUrns.filter((g) => g !== group);
        setGroupUrns(newGroupActors as never[]);
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
            <div style={{ margin: 12 }}>
                {entityRegistry.renderPreview(result.entity.type, PreviewType.MINI_SEARCH, result.entity)}
            </div>
        );
    };

    const userSearchResults = userSearchData?.search?.searchResults;
    const groupSearchResults = groupSearchData?.search?.searchResults;

    return (
        <Form layout="vertical" initialValues={{}} style={{ margin: 12, marginTop: 36, marginBottom: 40 }}>
            <Typography.Title level={4}>Applies to</Typography.Title>
            <Typography.Paragraph>
                Select the users & groups that this policy should be applied to.
            </Typography.Paragraph>
            <Form.Item label={<Typography.Text strong>Owners</Typography.Text>} labelAlign="right">
                <Typography.Paragraph>
                    Whether this policy should be apply to owners of the Metadata asset. If true, those who are marked
                    as owners of a Metadata Asset, either directly or indirectly via a Group, will have the selected
                    privileges.
                </Typography.Paragraph>
                <Switch size="small" checked={appliesToOwners} onChange={onToggleAppliesToOwners} />
            </Form.Item>
            <Form.Item label={<Typography.Text strong>Users</Typography.Text>}>
                <Typography.Paragraph>
                    Search for specific users that this policy should apply to, or select `All Users` to apply it to all
                    users.
                </Typography.Paragraph>
                <Select
                    defaultValue={userUrns}
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
                    <Select.Option value="all">All Users</Select.Option>
                </Select>
            </Form.Item>
            <Form.Item label={<Typography.Text strong>Groups</Typography.Text>}>
                <Typography.Paragraph>
                    Search for specific groups that this policy should apply to, or select `All Groups` to apply it to
                    all groups.
                </Typography.Paragraph>
                <Select
                    defaultValue={groupUrns}
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
                    <Select.Option value="all">All Groups</Select.Option>
                </Select>
            </Form.Item>
        </Form>
    );
}
