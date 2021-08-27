import React from 'react';
import { Form, Select, Tag, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { useEntityRegistry } from '../useEntityRegistry';
import { useGetSearchResultsLazyQuery } from '../../graphql/search.generated';
import { EntityType, ResourceFilter, PolicyType } from '../../types.generated';
import { RESOURCE_TYPES, RESOURCE_PRIVILEGES, PLATFORM_PRIVILEGES } from './privileges';

const typeToPrivileges = (policyType, type) => {
    // If we are dealing with a Platform policy, only show platform privileges.
    if (policyType === PolicyType.Platform) {
        return PLATFORM_PRIVILEGES;
    }
    // Otherwise, show resource-specific privileges.
    return RESOURCE_PRIVILEGES.filter((resourcePrivs) => resourcePrivs.resourceType === type).map(
        (resourcePrivs) => resourcePrivs.privileges,
    )[0];
};

type Props = {
    policyType: PolicyType;
    resources: ResourceFilter;
    setResources: (resources: ResourceFilter) => void;
    privileges: Array<string>;
    setPrivileges: (newPrivs: Array<string>) => void;
};

/**
 * This is used for search - it allows you to map an asset resource to a search type.
 * By default, we simply assume a 1:1 correspondence between resource name and EntityType.
 */
const mapResourceToEntityType = (resource: string) => {
    return resource as EntityType;
};

export default function PolicyPrivilegeForm({ policyType, resources, setResources, privileges, setPrivileges }: Props) {
    const [search, { data: searchData, loading: searchLoading }] = useGetSearchResultsLazyQuery();

    const entityRegistry = useEntityRegistry();
    const assetSearchResults = searchData?.search?.searchResults;
    const privilegeOptions = typeToPrivileges(policyType, resources.type);

    const onSelectPrivilege = (privilege: string) => {
        if (privilege === 'All') {
            setPrivileges(privilegeOptions.map((priv) => priv.type) as never[]);
        } else {
            const newPrivs = [...privileges, privilege];
            setPrivileges(newPrivs as never[]);
        }
    };

    const onDeselectPrivilege = (privilege: string) => {
        let newPrivs;
        if (privilege === 'All') {
            newPrivs = [];
        } else {
            newPrivs = privileges.filter((priv) => priv !== privilege);
        }
        setPrivileges(newPrivs as never[]);
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

    const showResourceTypeInput = policyType !== PolicyType.Platform;
    const showResourceInput = showResourceTypeInput && resources.type !== '';
    const showPrivilegesInput =
        policyType === PolicyType.Platform ||
        resources.allResources ||
        (resources.resources && resources.resources?.length > 0);

    const resourceSelectValue = resources.allResources ? ['All'] : resources.resources || [];
    const privilegesSelectValue = privileges;

    return (
        <Form layout="vertical" initialValues={{}} style={{ margin: 12, marginTop: 36, marginBottom: 40 }}>
            {showResourceTypeInput && (
                <Form.Item label={<Typography.Text strong>Asset Type</Typography.Text>} labelAlign="right">
                    <Typography.Paragraph>
                        Select the specific type of asset this policy should apply to.
                    </Typography.Paragraph>
                    <Select defaultValue={resources.type} onSelect={(type) => setResources({ ...resources, type })}>
                        {RESOURCE_TYPES.map((resourceType) => {
                            return <Select.Option value={resourceType.type}>{resourceType.displayName}</Select.Option>;
                        })}
                    </Select>
                </Form.Item>
            )}
            {showResourceInput && (
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
                                    <div style={{ margin: 12, display: 'flex', justifyContent: 'space-between' }}>
                                        {entityRegistry.getDisplayName(result.entity.type, result.entity)}
                                        <Link
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            to={() =>
                                                `/${entityRegistry.getPathName(result.entity.type)}/${
                                                    result.entity.urn
                                                }`
                                            }
                                        >
                                            View
                                        </Link>
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
            {showPrivilegesInput && (
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
