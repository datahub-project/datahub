import { Select, Tag } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import DomainNavigator from '@app/domainV2/nestedDomains/domainNavigator/DomainNavigator';
import { createCriterionValueWithEntity, getFieldValues, setFieldValues } from '@app/permissions/policy/policyUtils';
import ClickOutside from '@app/shared/ClickOutside';
import { BrowserWrapper } from '@app/shared/tags/AddTagsTermsModal';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsLazyQuery } from '@graphql/search.generated';
import { Domain, EntityType, PolicyMatchCriterionValue, ResourceFilter } from '@types';

const DOMAIN_FILTER_FIELD_NAME = 'DOMAIN';

const SearchResultContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 4px;
`;

type Props = {
    resources: ResourceFilter;
    setResources: (resources: ResourceFilter) => void;
};

export function DomainSelector({ resources, setResources }: Props) {
    const entityRegistry = useEntityRegistry();
    const [domainInputValue, setDomainInputValue] = useState('');
    const [isFocusedOnInput, setIsFocusedOnInput] = useState(false);

    const [searchDomains, { data: domainsSearchData }] = useGetSearchResultsLazyQuery();
    const domainSearchResults = domainsSearchData?.search?.searchResults;

    const domains = getFieldValues(resources.filter, DOMAIN_FILTER_FIELD_NAME) || [];
    const domainUrnToDisplayName = new Map();
    domains.forEach((domainEntity) => {
        const displayname = domainEntity.entity
            ? entityRegistry.getDisplayName(domainEntity.entity.type, domainEntity.entity)
            : domainEntity.value;
        domainUrnToDisplayName[domainEntity.value] = displayname;
    });

    const domainSelectValue = domains.map((criterionValue) => criterionValue.value);
    const isShowingDomainNavigator = !domainInputValue && isFocusedOnInput;

    const getEntityFromSearchResults = (searchResults, urn) =>
        searchResults?.map((result) => result.entity).find((entity) => entity.urn === urn);

    const onSelectDomain = (domainUrn, domainObj?: Domain) => {
        const filter = resources.filter || {
            criteria: [],
        };
        const domainEntity = domainObj || getEntityFromSearchResults(domainSearchResults, domainUrn);

        const isDomainAlreadyAdded = domains.some((item) => item.value === domainUrn);

        let updatedDomainEntities: PolicyMatchCriterionValue[] = [];

        if (isDomainAlreadyAdded) {
            updatedDomainEntities = domains.filter((item) => item.value !== domainUrn);
        } else {
            updatedDomainEntities = [...domains, createCriterionValueWithEntity(domainUrn, domainEntity)];
        }

        const updatedFilter = setFieldValues(filter, DOMAIN_FILTER_FIELD_NAME, updatedDomainEntities);
        setResources({
            ...resources,
            filter: updatedFilter,
        });
    };

    function selectDomainFromBrowser(domain: Domain) {
        onSelectDomain(domain.urn, domain);
        setIsFocusedOnInput(false);
    }

    const onDeselectDomain = (domain) => {
        const filter = resources.filter || {
            criteria: [],
        };
        setResources({
            ...resources,
            filter: setFieldValues(
                filter,
                DOMAIN_FILTER_FIELD_NAME,
                domains?.filter((criterionValue) => criterionValue.value !== domain),
            ),
        });
    };

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

    function handleBlur() {
        setDomainInputValue('');
    }

    function handleCLickOutside() {
        // delay closing the domain navigator so we don't get a UI "flash" between showing search results and navigator
        setTimeout(() => setIsFocusedOnInput(false), 0);
    }

    return (
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
                <DomainNavigator selectDomainOverride={selectDomainFromBrowser} selectedUrns={domainSelectValue} />
            </BrowserWrapper>
        </ClickOutside>
    );
}
