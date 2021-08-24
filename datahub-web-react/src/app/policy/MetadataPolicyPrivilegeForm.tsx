import React from 'react';
import { Form, Select, Tag, Typography } from 'antd';
import { useEntityRegistry } from '../useEntityRegistry';
import { useGetSearchResultsLazyQuery } from '../../graphql/search.generated';
import { PreviewType } from '../entity/Entity';
import { EntityType, ResourceFilter } from '../../types.generated';
import { RESOURCE_TYPES, RESOURCE_PRIVILEGES } from './privileges';

const typeToPrivileges = (type) => {
    return RESOURCE_PRIVILEGES.filter((resourcePrivs) => resourcePrivs.resourceType === type).map(
        (resourcePrivs) => resourcePrivs.privileges,
    )[0];
};

type Props = {
    resources: ResourceFilter;
    setResources: (resources: ResourceFilter) => void;
    privileges: Array<string>;
    setPrivileges: (newPrivs: Array<string>) => void;
    updateStepCompletion: (isComplete: boolean) => void;
};

/**
 * This is used for search - it allows you to map an asset resource to a search type.
 * By default, we simply assume a 1:1 correspondence between resource name and EntityType.
 */
const mapResourceToEntityType = (resource: string) => {
    return resource as EntityType;
};

export default function MetadataPolicyPrivilegeForm({
    resources,
    setResources,
    privileges,
    setPrivileges,
    updateStepCompletion,
}: Props) {
    const [search, { data: searchData, loading: searchLoading }] = useGetSearchResultsLazyQuery();

    const entityRegistry = useEntityRegistry();
    const assetSearchResults = searchData?.search?.searchResults;
    const privilegeOptions = typeToPrivileges(resources.type);

    const onSelectPrivilege = (privilege: string) => {
        if (privilege === 'All') {
            setPrivileges(privilegeOptions.map((priv) => priv.type) as never[]);
        } else {
            const newPrivs = [...privileges, privilege];
            setPrivileges(newPrivs as never[]);
        }
        updateStepCompletion(true);
    };

    const onDeselectPrivilege = (privilege: string) => {
        let newPrivs;
        if (privilege === 'All') {
            newPrivs = [];
        } else {
            newPrivs = privileges.filter((priv) => priv !== privilege);
        }
        setPrivileges(newPrivs as never[]);
        if (newPrivs.length === 0) {
            updateStepCompletion(false);
        }
    };

    const onSelectAsset = (asset: string) => {
        if (asset === 'All') {
            setResources({
                ...resources,
                allResources: true,
            });
        } else {
            const newAssets = [...(resources.resources || []), asset as string];
            setResources({
                ...resources,
                resources: newAssets,
            });
        }
    };

    const onDeselectAsset = (asset: string) => {
        if (asset === 'All') {
            setResources({
                ...resources,
                allResources: false,
            });
        } else {
            const newAssets = resources.resources?.filter((urn) => urn !== asset);
            setResources({
                ...resources,
                resources: newAssets,
            });
        }
    };

    const handleSearch = (event: any) => {
        const text = event.target.value as string;
        if (text.length > 2) {
            // Now we search.. notice that permissioned entities need to be searchable.
            search({
                variables: {
                    input: {
                        type: mapResourceToEntityType(resources.type),
                        query: text,
                        start: 0,
                        count: 10,
                    },
                },
            });
        }
    };

    const showResourcesStep = resources.type !== '';
    const showPrivilegesStep = resources.allResources || (resources.resources && resources.resources?.length > 0);

    const resourceSelectValue = resources.allResources ? ['All'] : resources.resources || [];
    const privilegesSelectValue = privileges;

    return (
        <Form layout="vertical" initialValues={{}} style={{ margin: 12, marginTop: 36, marginBottom: 40 }}>
            <Form.Item label={<Typography.Text strong>Asset Type</Typography.Text>} labelAlign="right">
                <Typography.Paragraph>
                    Select the specific type of asset this policy should apply to, or all if it should apply to all
                    assets.
                </Typography.Paragraph>
                <Select defaultValue={resources.type} onSelect={(type) => setResources({ ...resources, type })}>
                    {RESOURCE_TYPES.map((resourceType) => {
                        return <Select.Option value={resourceType.type}>{resourceType.displayName}</Select.Option>;
                    })}
                </Select>
            </Form.Item>
            {showResourcesStep && (
                <Form.Item label={<Typography.Text strong>Asset</Typography.Text>}>
                    <Typography.Paragraph>
                        Search for specific assets the policy should apply to, or select <b>All</b> to apply the policy
                        to all assets of the given type.
                    </Typography.Paragraph>
                    <Select
                        value={resourceSelectValue}
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
                        <Select.Option value="All">{`All ${entityRegistry.getCollectionName(
                            mapResourceToEntityType(resources.type),
                        )}`}</Select.Option>
                    </Select>
                </Form.Item>
            )}
            {showPrivilegesStep && (
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
                            value={privilegesSelectValue}
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
                            <Select.Option value="All">All Privileges</Select.Option>
                        </Select>
                    </Form.Item>
                </>
            )}
        </Form>
    );
}
