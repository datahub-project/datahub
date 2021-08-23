import React from 'react';
import { Form, Select, Tag, Typography } from 'antd';
import { ENTITY_PRIVILEGES } from './entityPrivileges';
import { useEntityRegistry } from '../useEntityRegistry';
import { useGetSearchResultsLazyQuery } from '../../graphql/search.generated';
import { PreviewType } from '../entity/Entity';
import { EntityType } from '../../types.generated';

const typeToPrivileges = (type) => {
    return ENTITY_PRIVILEGES.filter((entityPrivs) => entityPrivs.entityType === type).map(
        (entityPrivs) => entityPrivs.privileges,
    )[0];
};

type Props = {
    assetType: string;
    setAssetType: (assetType: string) => void;
    assetUrns: Array<string>;
    setAssetUrns: (newUrns: Array<string>) => void;
    privileges: Array<string>;
    setPrivileges: (newPrivs: Array<string>) => void;
    updateStepCompletion: (isComplete: boolean) => void;
};

export default function MetadataPolicyPrivilegeForm({
    assetType,
    setAssetType,
    assetUrns,
    setAssetUrns,
    privileges,
    setPrivileges,
    updateStepCompletion,
}: Props) {
    const [search, { data: searchData, loading: searchLoading }] = useGetSearchResultsLazyQuery();

    const entityRegistry = useEntityRegistry();

    const onSelectPrivilege = (privilege: string) => {
        updateStepCompletion(true);
        const newPrivs = [...privileges, privilege];
        setPrivileges(newPrivs as never[]);
    };

    const onDeselectPrivilege = (privilege: string) => {
        const newPrivs = privileges.filter((priv) => priv !== privilege);
        setPrivileges(newPrivs as never[]);
        if (newPrivs.length === 0) {
            updateStepCompletion(false);
        }
    };

    const onSelectAsset = (asset: string) => {
        const newAssets = [...assetUrns, asset as string];
        setAssetUrns(newAssets as never[]);
    };

    const onDeselectAsset = (asset: string) => {
        const newAssets = assetUrns.filter((urn) => urn !== asset);
        setAssetUrns(newAssets as never[]);
    };

    const handleSearch = (event: any) => {
        const text = event.target.value as string;
        console.log(text);
        if (text.length > 2) {
            // Now we search.. notice that permissioned entities need to be searchable.
            search({
                variables: {
                    input: {
                        type: assetType as EntityType,
                        query: text,
                        start: 0,
                        count: 10,
                    },
                },
            });
        }
    };

    const assetSearchResults = searchData?.search?.searchResults;
    const privilegeOptions = typeToPrivileges(assetType);

    return (
        <Form layout="vertical" initialValues={{}} style={{ margin: 12, marginTop: 36, marginBottom: 40 }}>
            <Form.Item label={<Typography.Text strong>Asset Type</Typography.Text>} labelAlign="right">
                <Typography.Paragraph>
                    Select the specific type of asset this policy should apply to, or all if it should apply to all
                    assets.
                </Typography.Paragraph>
                <Select defaultValue={assetType} onSelect={(type) => setAssetType(type as EntityType)}>
                    {entityRegistry.getSearchEntityTypes().map((type) => {
                        return (
                            <Select.Option value={type}>
                                {entityRegistry.getEntity(type).getCollectionName()}
                            </Select.Option>
                        );
                    })}
                </Select>
            </Form.Item>
            {assetType && (
                <Form.Item label={<Typography.Text strong>Asset</Typography.Text>}>
                    <Typography.Paragraph>
                        Search for specific assets the policy should apply to, or select <b>All</b> to apply the policy
                        to all assets of the given type.
                    </Typography.Paragraph>
                    <Select
                        defaultValue={assetUrns}
                        mode="multiple"
                        placeholder="Search for data assets..."
                        onSelect={(asset: any) => onSelectAsset(asset)}
                        onDeselect={(asset: any) => onDeselectAsset(asset)}
                        onInputKeyDown={handleSearch}
                        tagRender={(tagProps) => (
                            <Tag closable={tagProps.closable} onClose={tagProps.onClose}>
                                {tagProps.value}
                            </Tag>
                        )}
                    >
                        {assetSearchResults &&
                            assetSearchResults.map((result) => (
                                <Select.Option value={result.entity.urn}>
                                    <div style={{ margin: 12 }}>
                                        {entityRegistry.renderPreview(
                                            result.entity.type,
                                            PreviewType.MINI_SEARCH,
                                            result.entity,
                                        )}
                                    </div>
                                </Select.Option>
                            ))}
                        {searchLoading && <Select.Option value="loading">Searching...</Select.Option>}
                        <Select.Option value="all">{`All ${entityRegistry.getCollectionName(
                            assetType as EntityType,
                        )}`}</Select.Option>
                    </Select>
                </Form.Item>
            )}
            {assetUrns.length > 0 && (
                <>
                    <Form.Item
                        label={<Typography.Text strong>Privileges</Typography.Text>}
                        rules={[
                            {
                                required: true,
                                message: 'Please select your permissions.',
                                type: 'array',
                            },
                        ]}
                    >
                        <Typography.Paragraph>Select a set of privileges to permit.</Typography.Paragraph>
                        <Select
                            defaultValue={privileges}
                            mode="multiple"
                            onSelect={(value: string) => onSelectPrivilege(value)}
                            onDeselect={(value: any) => onDeselectPrivilege(value)}
                            tagRender={(tagProps) => (
                                <Tag closable={tagProps.closable} onClose={tagProps.onClose}>
                                    {tagProps.label}
                                </Tag>
                            )}
                        >
                            {privilegeOptions.map((priv) => (
                                <Select.Option value={priv.type}>{priv.displayName}</Select.Option>
                            ))}
                            <Select.Option value="all">All Privileges</Select.Option>
                        </Select>
                    </Form.Item>
                </>
            )}
        </Form>
    );
}
