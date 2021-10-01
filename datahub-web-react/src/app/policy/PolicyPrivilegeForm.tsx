import React from 'react';
import { Form, Select, Tag, Typography } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityRegistry } from '../useEntityRegistry';
import { useAppConfig } from '../useAppConfig';
import { useGetSearchResultsLazyQuery } from '../../graphql/search.generated';
import { ResourceFilter, PolicyType } from '../../types.generated';
import {
    EMPTY_POLICY,
    mapResourceTypeToDisplayName,
    mapResourceTypeToEntityType,
    mapResourceTypeToPrivileges,
} from './policyUtils';

type Props = {
    policyType: PolicyType;
    resources?: ResourceFilter;
    setResources: (resources: ResourceFilter) => void;
    privileges: Array<string>;
    setPrivileges: (newPrivs: Array<string>) => void;
};

const PrivilegesForm = styled(Form)`
    margin: 12px;
    margin-top: 36px;
    margin-bottom: 40px;
`;

const SearchResultContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px;
`;

/**
 * Component used to construct the "privileges" and "resources" portion of a DataHub
 * access Policy.
 */
export default function PolicyPrivilegeForm({
    policyType,
    resources: maybeResources,
    setResources,
    privileges,
    setPrivileges,
}: Props) {
    const entityRegistry = useEntityRegistry();

    // Configuration used for displaying options
    const {
        config: { policiesConfig },
    } = useAppConfig();

    // Search for resources
    const [search, { data: searchData }] = useGetSearchResultsLazyQuery();
    const resourceSearchResults = searchData?.search?.searchResults;

    const resources = maybeResources || EMPTY_POLICY.resources;

    // Whether to show the "resource type" input.
    const showResourceTypeInput = policyType !== PolicyType.Platform;

    // Whether to show the "resource" input.
    const showResourceInput = showResourceTypeInput && resources.type !== '';

    // Whether to show the "privileges" input.
    const showPrivilegesInput =
        policyType === PolicyType.Platform ||
        resources.allResources ||
        (resources.resources && resources.resources?.length > 0);

    // Construct privilege options for dropdown
    const platformPrivileges = policiesConfig?.platformPrivileges || [];
    const resourcePrivileges = policiesConfig?.resourcePrivileges || [];
    const privilegeOptions =
        policyType === PolicyType.Platform
            ? platformPrivileges
            : mapResourceTypeToPrivileges(resources.type, resourcePrivileges);

    // Current Select dropdown values
    const resourceTypeSelectValue = resources.type;
    const resourceSelectValue = resources.allResources ? ['All'] : resources.resources || [];
    const privilegesSelectValue = privileges;

    // When a privilege is selected, add its type to the privileges list
    const onSelectPrivilege = (privilege: string) => {
        if (privilege === 'All') {
            setPrivileges(privilegeOptions.map((priv) => priv.type) as never[]);
        } else {
            const newPrivs = [...privileges, privilege];
            setPrivileges(newPrivs as never[]);
        }
    };

    // When a privilege is selected, remove its type from the privileges list
    const onDeselectPrivilege = (privilege: string) => {
        let newPrivs;
        if (privilege === 'All') {
            newPrivs = [];
        } else {
            newPrivs = privileges.filter((priv) => priv !== privilege);
        }
        setPrivileges(newPrivs as never[]);
    };

    // When a resource is selected, add its urn to the list of resources
    const onSelectResource = (resource: string) => {
        if (resource === 'All') {
            setResources({
                ...resources,
                allResources: true,
            });
        } else {
            const newAssets = [...(resources.resources || []), resource as string];
            setResources({
                ...resources,
                resources: newAssets,
            });
        }
    };

    // When a resource is deselected, remove its urn from the list of resources
    const onDeselectResource = (resource: string) => {
        if (resource === 'All') {
            setResources({
                ...resources,
                allResources: false,
            });
        } else {
            const newAssets = resources.resources?.filter((urn) => urn !== resource);
            setResources({
                ...resources,
                resources: newAssets,
            });
        }
    };

    // Handle resource search, if the resource type has an associated EntityType mapping.
    const handleSearch = (text: string) => {
        const maybeEntityType = mapResourceTypeToEntityType(resources.type, resourcePrivileges);
        if (maybeEntityType) {
            if (text.length > 2) {
                search({
                    variables: {
                        input: {
                            type: maybeEntityType,
                            query: text,
                            start: 0,
                            count: 10,
                        },
                    },
                });
            }
        }
    };

    const renderSearchResult = (result) => {
        return (
            <SearchResultContainer>
                {entityRegistry.getDisplayName(result.entity.type, result.entity)}
                <Link
                    target="_blank"
                    rel="noopener noreferrer"
                    to={() => `/${entityRegistry.getPathName(result.entity.type)}/${result.entity.urn}`}
                >
                    View
                </Link>
            </SearchResultContainer>
        );
    };

    const selectedResourceDisplayName =
        showResourceInput && mapResourceTypeToDisplayName(resources.type, resourcePrivileges);

    return (
        <PrivilegesForm layout="vertical">
            {showResourceTypeInput && (
                <Form.Item label={<Typography.Text strong>Resource Type</Typography.Text>} labelAlign="right">
                    <Typography.Paragraph>
                        Select the specific type of resource this policy should apply to.
                    </Typography.Paragraph>
                    <Select value={resourceTypeSelectValue} onSelect={(type) => setResources({ ...resources, type })}>
                        {resourcePrivileges.map((resPrivs) => {
                            return (
                                <Select.Option value={resPrivs.resourceType}>
                                    {resPrivs.resourceTypeDisplayName}
                                </Select.Option>
                            );
                        })}
                    </Select>
                </Form.Item>
            )}
            {showResourceInput && (
                <Form.Item label={<Typography.Text strong>Resource</Typography.Text>}>
                    <Typography.Paragraph>
                        Search for specific resources the policy should apply to, or select <b>All</b> to apply the
                        policy to all resources of the given type.
                    </Typography.Paragraph>
                    <Select
                        value={resourceSelectValue}
                        mode="multiple"
                        placeholder={`Search for ${selectedResourceDisplayName}...`}
                        onSelect={(asset: any) => onSelectResource(asset)}
                        onDeselect={(asset: any) => onDeselectResource(asset)}
                        onSearch={handleSearch}
                        tagRender={(tagProps) => (
                            <Tag closable={tagProps.closable} onClose={tagProps.onClose}>
                                {tagProps.value}
                            </Tag>
                        )}
                    >
                        {resourceSearchResults?.map((result) => (
                            <Select.Option value={result.entity.urn}>{renderSearchResult(result)}</Select.Option>
                        ))}
                        <Select.Option value="All">{`All ${selectedResourceDisplayName}`}</Select.Option>
                    </Select>
                </Form.Item>
            )}
            {showPrivilegesInput && (
                <Form.Item label={<Typography.Text strong>Privileges</Typography.Text>}>
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
            )}
        </PrivilegesForm>
    );
}
