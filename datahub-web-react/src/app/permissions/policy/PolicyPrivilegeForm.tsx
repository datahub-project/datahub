import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { Form, Select, Tag, Tooltip, Typography, Tag as CustomTag } from 'antd';
import styled from 'styled-components/macro';

import { useEntityRegistry } from '../../useEntityRegistry';
import { useAppConfig } from '../../useAppConfig';
import {
    useGetSearchResultsForMultipleLazyQuery,
    useGetSearchResultsLazyQuery,
} from '../../../graphql/search.generated';
import { ResourceFilter, PolicyType, EntityType, Domain, Entity } from '../../../types.generated';
import {
    convertLegacyResourceFilter,
    createCriterionValue,
    createCriterionValueWithEntity,
    EMPTY_POLICY,
    getFieldValues,
    getFieldValuesOfTags,
    mapResourceTypeToDisplayName,
    mapResourceTypeToEntityType,
    mapResourceTypeToPrivileges,
    setFieldValues,
} from './policyUtils';
import DomainNavigator from '../../domain/nestedDomains/domainNavigator/DomainNavigator';
import { BrowserWrapper } from '../../shared/tags/AddTagsTermsModal';
import ClickOutside from '../../shared/ClickOutside';
import { TagTermLabel } from '../../shared/tags/TagTermLabel';
import { ENTER_KEY_CODE } from '../../shared/constants';
import { useGetRecommendations } from '../../shared/recommendation';
import { RESOURCE_TYPE, RESOURCE_URN, TYPE, URN } from './constants';

type Props = {
    policyType: PolicyType;
    resources?: ResourceFilter;
    setResources: (resources: ResourceFilter) => void;
    selectedTags?: any[];
    setSelectedTags: (data: any) => void;
    setEditState: (data: boolean) => void;
    isEditState: boolean;
    privileges: Array<string>;
    setPrivileges: (newPrivs: Array<string>) => void;
    focusPolicyUrn: string | undefined;
};

const SearchResultContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 4px;
`;

const PrivilegesForm = styled(Form)`
    margin: 12px;
    margin-top: 36px;
    margin-bottom: 40px;
`;

const TagSelect = styled(Select)`
    width: 480px;
`;

const StyleTag = styled(CustomTag)`
    margin: 2px;
    display: flex;
    justify-content: start;
    align-items: center;
    white-space: nowrap;
    opacity: 1;
    color: #434343;
    line-height: 16px;
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
    setSelectedTags,
    selectedTags,
    setEditState,
    isEditState,
    focusPolicyUrn,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const [domainInputValue, setDomainInputValue] = useState('');
    const [isFocusedOnInput, setIsFocusedOnInput] = useState(false);
    const [inputValue, setInputValue] = useState('');
    const [tagTermSearch, { data: tagTermSearchData }] = useGetSearchResultsLazyQuery();
    const [recommendedData] = useGetRecommendations([EntityType.Tag]);
    const tagSearchResults = tagTermSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];

    const inputEl = useRef(null);

    // Configuration used for displaying options
    const {
        config: { policiesConfig },
    } = useAppConfig();

    const resources: ResourceFilter = convertLegacyResourceFilter(maybeResources) || EMPTY_POLICY.resources;
    // RESOURCE_TYPE and RESOURCE_URN are deprecated, but need to get them for backwards compatibility
    const resourceTypes = getFieldValues(resources.filter, TYPE, RESOURCE_TYPE) || [];
    const resourceEntities = getFieldValues(resources.filter, URN, RESOURCE_URN) || [];

    const getDisplayName = (entity) => {
        if (!entity) {
            return null;
        }
        return entityRegistry.getDisplayName(entity.type, entity);
    };

    const resourceUrnToDisplayName = new Map();
    resourceEntities.forEach((resourceEntity) => {
        resourceUrnToDisplayName[resourceEntity.value] = getDisplayName(resourceEntity.entity);
    });
    // Search for resources
    const [searchResources, { data: resourcesSearchData }] = useGetSearchResultsForMultipleLazyQuery();
    const resourceSearchResults = resourcesSearchData?.searchAcrossEntities?.searchResults;

    // Same for domains
    const domains = getFieldValues(resources.filter, 'DOMAIN') || [];
    const domainUrnToDisplayName = new Map();
    domains.forEach((domainEntity) => {
        domainUrnToDisplayName[domainEntity.value] = getDisplayName(domainEntity.entity);
    });
    // Search for domains
    const [searchDomains, { data: domainsSearchData }] = useGetSearchResultsLazyQuery();
    const domainSearchResults = domainsSearchData?.search?.searchResults;

    // Whether to show the resource filter inputs including "resource type", "resource", and "domain"
    const showResourceFilterInput = policyType !== PolicyType.Platform;

    // Current Select dropdown values
    const resourceTypeSelectValue = resourceTypes.map((criterionValue) => criterionValue.value);
    const resourceSelectValue = resourceEntities.map((criterionValue) => criterionValue.value);
    const domainSelectValue = getFieldValues(resources.filter, 'DOMAIN').map((criterionValue) => criterionValue.value);
    const privilegesSelectValue = privileges;
    const isShowingDomainNavigator = !domainInputValue && isFocusedOnInput;

    // Construct privilege options for dropdown
    const platformPrivileges = policiesConfig?.platformPrivileges || [];
    const resourcePrivileges = useMemo(() => policiesConfig?.resourcePrivileges || [], [policiesConfig]);
    const resourcePrivilegesForType = useMemo(
        () => mapResourceTypeToPrivileges(resourceTypeSelectValue, resourcePrivileges),
        [resourceTypeSelectValue, resourcePrivileges],
    );
    const privilegeOptions = policyType === PolicyType.Platform ? platformPrivileges : resourcePrivilegesForType;

    const getEntityFromSearchResults = (searchResults, urn) =>
        searchResults?.map((result) => result.entity).find((entity) => entity.urn === urn);

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
    const onSelectResourceType = (selectedResourceType: string) => {
        const filter = resources.filter || {
            criteria: [],
        };
        // remove the deprecated RESOURCE_TYPE field and replace with TYPE field
        const filterWithoutDeprecatedField = setFieldValues(filter, RESOURCE_TYPE, []);
        setResources({
            ...resources,
            filter: setFieldValues(filterWithoutDeprecatedField, TYPE, [
                ...resourceTypes,
                createCriterionValue(selectedResourceType),
            ]),
        });
    };

    const onDeselectResourceType = (deselectedResourceType: string) => {
        const filter = resources.filter || {
            criteria: [],
        };
        // remove the deprecated RESOURCE_TYPE field and replace with TYPE field
        const filterWithoutDeprecatedField = setFieldValues(filter, RESOURCE_TYPE, []);
        setResources({
            ...resources,
            filter: setFieldValues(
                filterWithoutDeprecatedField,
                TYPE,
                resourceTypes?.filter((criterionValue) => criterionValue.value !== deselectedResourceType),
            ),
        });
    };

    // When a resource is selected, add its urn to the list of resources
    const onSelectResource = (resource) => {
        const filter = resources.filter || {
            criteria: [],
        };
        // remove the deprecated RESOURCE_URN field and replace with URN field
        const filterWithoutDeprecatedField = setFieldValues(filter, RESOURCE_URN, []);
        setResources({
            ...resources,
            filter: setFieldValues(filterWithoutDeprecatedField, URN, [
                ...resourceEntities,
                createCriterionValueWithEntity(
                    resource,
                    getEntityFromSearchResults(resourceSearchResults, resource) || null,
                ),
            ]),
        });
    };

    // When a resource is deselected, remove its urn from the list of resources
    const onDeselectResource = (resource) => {
        const filter = resources.filter || {
            criteria: [],
        };
        // remove the deprecated RESOURCE_URN field and replace with URN field
        const filterWithoutDeprecatedField = setFieldValues(filter, RESOURCE_URN, []);
        setResources({
            ...resources,
            filter: setFieldValues(
                filterWithoutDeprecatedField,
                URN,
                resourceEntities?.filter((criterionValue) => criterionValue.value !== resource),
            ),
        });
    };

    // When a domain is selected, add its urn to the list of domains
    const onSelectDomain = (domainUrn, domainObj?: Domain) => {
        const filter = resources.filter || {
            criteria: [],
        };
        const domainEntity = domainObj || getEntityFromSearchResults(domainSearchResults, domainUrn);
        const updatedFilter = setFieldValues(filter, 'DOMAIN', [
            ...domains,
            createCriterionValueWithEntity(domainUrn, domainEntity || null),
        ]);
        setResources({
            ...resources,
            filter: updatedFilter,
        });
    };

    function selectDomainFromBrowser(domain: Domain) {
        onSelectDomain(domain.urn, domain);
        setIsFocusedOnInput(false);
    }

    // When a domain is deselected, remove its urn from the list of domains
    const onDeselectDomain = (domain) => {
        const filter = resources.filter || {
            criteria: [],
        };
        setResources({
            ...resources,
            filter: setFieldValues(
                filter,
                'DOMAIN',
                domains?.filter((criterionValue) => criterionValue.value !== domain),
            ),
        });
    };

    // Handle resource search, if the resource type has an associated EntityType mapping.
    const handleResourceSearch = (text: string) => {
        const trimmedText: string = text.trim();
        const entityTypes = resourceTypeSelectValue
            .map((resourceType) => mapResourceTypeToEntityType(resourceType, resourcePrivileges))
            .filter((entityType): entityType is EntityType => !!entityType);
        searchResources({
            variables: {
                input: {
                    types: entityTypes,
                    query: trimmedText.length > 2 ? trimmedText : '*',
                    start: 0,
                    count: 10,
                },
            },
        });
    };

    // Handle domain search, if the domain type has an associated EntityType mapping.
    const handleDomainSearch = (text: string) => {
        const trimmedText: string = text.trim();
        setDomainInputValue(trimmedText);
        searchDomains({
            variables: {
                input: {
                    type: EntityType.Domain,
                    query: trimmedText.length > 2 ? trimmedText : '*',
                    start: 0,
                    count: 10,
                },
            },
        });
    };

    const renderSearchResult = (result) => {
        return (
            <SearchResultContainer>
                {entityRegistry.getDisplayName(result.entity.type, result.entity)}
                <Link
                    target="_blank"
                    rel="noopener noreferrer"
                    to={() => `${entityRegistry.getEntityUrl(result.entity.type, result.entity.urn)}`}
                >
                    View
                </Link>
            </SearchResultContainer>
        );
    };

    const displayStringWithMaxLength = (displayStr, length) => {
        return displayStr.length > length
            ? `${displayStr.substring(0, Math.min(length, displayStr.length))}...`
            : displayStr;
    };

    function handleCLickOutside() {
        // delay closing the domain navigator so we don't get a UI "flash" between showing search results and navigator
        setTimeout(() => setIsFocusedOnInput(false), 0);
    }

    function handleBlur() {
        setDomainInputValue('');
    }

    function handleBlurTag() {
        setInputValue('');
    }

    const renderSearchResultTags = (entity: Entity) => {
        const displayName =
            entity.type === EntityType.Tag ? (entity as any).name : entityRegistry.getDisplayName(entity.type, entity);
        const tagOrTermComponent = <TagTermLabel entity={entity} />;
        return (
            <Select.Option data-testid="tag-term-option" value={entity.urn} key={entity.urn} name={displayName}>
                <SearchResultContainer>{tagOrTermComponent}</SearchResultContainer>
            </Select.Option>
        );
    };
    const tags = getFieldValues(resources.filter, 'TAG') || [];
    const newTag = getFieldValues(resources.filter, 'TAG').map((criterionValue) => {
        if (criterionValue?.value) {
            return criterionValue?.value;
        }
        return criterionValue;
    });

    const editTags = getFieldValuesOfTags(resources.filter, 'TAG').map((criterionValue) => {
        if (criterionValue?.value) {
            return criterionValue?.entity;
        }
        return criterionValue;
    });
    const tagResult = !inputValue || inputValue.length === 0 ? recommendedData : tagSearchResults;
    useEffect(() => {
        if (focusPolicyUrn && isEditState && setEditState && editTags && newTag) {
            setEditState(false);
            const filter = resources.filter || {
                criteria: [],
            };
            setSelectedTags(editTags);
            setResources({
                ...resources,
                filter: setFieldValues(filter, 'TAG', [...(newTag as any)]),
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [focusPolicyUrn, isEditState, setEditState, editTags, newTag]);

    const onSelectValue = (urn: string) => {
        const filter = resources.filter || {
            criteria: [],
        };
        const selectedTagOption = tagResult?.find((tag) => tag.urn === urn);

        setResources({
            ...resources,
            filter: setFieldValues(filter, 'TAG', [...tags, urn as any]),
        });
        setSelectedTags([...(selectedTags as any), selectedTagOption]);
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
    };

    // When a Tag search result is deselected, remove the Tags
    const onDeselectValue = (urn: string) => {
        const filter = resources.filter || {
            criteria: [],
        };
        setInputValue('');
        setSelectedTags(selectedTags?.filter((term) => term.urn !== urn));

        setResources({
            ...resources,
            filter: setFieldValues(
                filter,
                'TAG',
                tags?.filter((criterionValue) => (criterionValue as any) !== urn),
            ),
        });
    };

    const type = EntityType.Tag;
    const handleSearch = (text: string) => {
        if (text.length > 0) {
            tagTermSearch({
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

    const tagSearchOptions = tagResult?.map((result) => {
        return renderSearchResultTags(result);
    });

    function clearInput() {
        setInputValue('');
        setTimeout(() => setIsFocusedOnInput(true), 0); // call after click outside
    }

    const tagRender = (props) => {
        // eslint-disable-next-line react/prop-types
        const { closable, onClose, value } = props;
        const onPreventMouseDown = (event) => {
            event.preventDefault();
            event.stopPropagation();
        };

        const selectedItem = selectedTags?.find((term) => term?.urn === value);
        return (
            <StyleTag onMouseDown={onPreventMouseDown} closable={closable} onClose={onClose}>
                {selectedItem?.name}
            </StyleTag>
        );
    };

    function handleKeyDown(event) {
        if (event.keyCode === ENTER_KEY_CODE) {
            (inputEl.current as any).blur();
        }
    }

    return (
        <PrivilegesForm layout="vertical">
            {showResourceFilterInput && (
                <Form.Item label={<Typography.Text strong>Resource Type</Typography.Text>} labelAlign="right">
                    <Typography.Paragraph>
                        Select the types of resources this policy should apply to. If <b>none</b> is selected, policy is
                        applied to <b>all</b> types of resources.
                    </Typography.Paragraph>
                    <Select
                        value={resourceTypeSelectValue}
                        mode="multiple"
                        placeholder="Apply to ALL resource types by default. Select types to apply to specific types of entities."
                        onSelect={onSelectResourceType}
                        onDeselect={onDeselectResourceType}
                        tagRender={(tagProps) => (
                            <Tag closable={tagProps.closable} onClose={tagProps.onClose}>
                                {mapResourceTypeToDisplayName(tagProps.value.toString(), resourcePrivileges)}
                            </Tag>
                        )}
                    >
                        {resourcePrivileges
                            .filter((privs) => privs.resourceType !== 'all')
                            .map((resPrivs) => {
                                return (
                                    <Select.Option key={resPrivs.resourceType} value={resPrivs.resourceType}>
                                        {resPrivs.resourceTypeDisplayName}
                                    </Select.Option>
                                );
                            })}
                    </Select>
                </Form.Item>
            )}
            {showResourceFilterInput && (
                <Form.Item label={<Typography.Text strong>Resource</Typography.Text>}>
                    <Typography.Paragraph>
                        Search for specific resources the policy should apply to. If <b>none</b> is selected, policy is
                        applied to <b>all</b> resources of the given type.
                    </Typography.Paragraph>
                    <Select
                        notFoundContent="No search results found"
                        value={resourceSelectValue}
                        mode="multiple"
                        filterOption={false}
                        placeholder="Apply to ALL resources by default. Select specific resources to apply to."
                        onSelect={onSelectResource}
                        onDeselect={onDeselectResource}
                        onSearch={handleResourceSearch}
                        tagRender={(tagProps) => (
                            <Tag closable={tagProps.closable} onClose={tagProps.onClose}>
                                <Tooltip title={tagProps.value.toString()}>
                                    {displayStringWithMaxLength(
                                        resourceUrnToDisplayName[tagProps.value.toString()] ||
                                            tagProps.value.toString(),
                                        75,
                                    )}
                                </Tooltip>
                            </Tag>
                        )}
                    >
                        {resourceSearchResults?.map((result) => (
                            <Select.Option key={result.entity.urn} value={result.entity.urn}>
                                {renderSearchResult(result)}
                            </Select.Option>
                        ))}
                    </Select>
                </Form.Item>
            )}
            {showResourceFilterInput && (
                <Form.Item label={<Typography.Text strong>Select Tags</Typography.Text>}>
                    <Typography.Paragraph>
                        The policy will apply to all entities containing all of the chosen tags. If no tags are
                        selected, the policy will not account for tags.
                    </Typography.Paragraph>
                    <TagSelect
                        data-testid="tag-term-modal-input"
                        mode="multiple"
                        ref={inputEl}
                        filterOption={false}
                        placeholder={`Search for ${entityRegistry.getEntityName(type)?.toLowerCase()}...`}
                        showSearch
                        defaultActiveFirstOption={false}
                        onSelect={(asset: any) => onSelectValue(asset)}
                        onDeselect={(asset: any) => onDeselectValue(asset)}
                        onSearch={(value: string) => {
                            // eslint-disable-next-line react/prop-types
                            handleSearch(value.trim());
                            // eslint-disable-next-line react/prop-types
                            setInputValue(value.trim());
                        }}
                        tagRender={tagRender}
                        value={tags}
                        onClear={clearInput}
                        onBlur={handleBlurTag}
                        onInputKeyDown={handleKeyDown}
                    >
                        {tagSearchOptions}
                    </TagSelect>
                </Form.Item>
            )}
            {showResourceFilterInput && (
                <Form.Item label={<Typography.Text strong>Select Domains</Typography.Text>}>
                    <Typography.Paragraph>
                        The policy will apply to any chosen domains and all their nested domains. If <b>none</b> are
                        selected, the policy is applied to <b>all</b> resources of in all domains.
                    </Typography.Paragraph>
                    <ClickOutside onClickOutside={handleCLickOutside}>
                        <Select
                            showSearch
                            value={domainSelectValue}
                            mode="multiple"
                            filterOption={false}
                            placeholder="Apply to ALL domains by default. Select domains to apply to specific domains."
                            onSelect={(value) => onSelectDomain(value)}
                            onDeselect={onDeselectDomain}
                            onSearch={handleDomainSearch}
                            onFocus={() => setIsFocusedOnInput(true)}
                            onBlur={handleBlur}
                            tagRender={(tagProps) => (
                                <Tag closable={tagProps.closable} onClose={tagProps.onClose}>
                                    {displayStringWithMaxLength(
                                        domainUrnToDisplayName[tagProps.value.toString()] || tagProps.value.toString(),
                                        75,
                                    )}
                                </Tag>
                            )}
                            dropdownStyle={isShowingDomainNavigator ? { display: 'none' } : {}}
                        >
                            {domainSearchResults?.map((result) => (
                                <Select.Option key={result.entity.urn} value={result.entity.urn}>
                                    {renderSearchResult(result)}
                                </Select.Option>
                            ))}
                        </Select>
                        <BrowserWrapper isHidden={!isShowingDomainNavigator} width="100%" maxHeight={300}>
                            <DomainNavigator selectDomainOverride={selectDomainFromBrowser} />
                        </BrowserWrapper>
                    </ClickOutside>
                </Form.Item>
            )}
            <Form.Item label={<Typography.Text strong>Privileges</Typography.Text>}>
                <Typography.Paragraph>Select a set of privileges to permit.</Typography.Paragraph>
                <Select
                    data-testid="privileges"
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
                    {privilegeOptions.map((priv, index) => {
                        const key = `${priv.type}-${index}`;
                        return (
                            <Select.Option key={key} value={priv.type}>
                                {priv.displayName}
                            </Select.Option>
                        );
                    })}
                    <Select.Option value="All">All Privileges</Select.Option>
                </Select>
            </Form.Item>
        </PrivilegesForm>
    );
}
