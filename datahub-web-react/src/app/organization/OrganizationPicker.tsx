import { LoadingOutlined } from '@ant-design/icons';
import { Empty, Select, Tag } from 'antd';
import React, { useRef } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { useGetSearchResultsLazyQuery } from '@graphql/search.generated';
import { OwnerLabel } from '@app/shared/OwnerLabel';
import { Entity, EntityType } from '@src/types.generated';
import { useEntityRegistry } from '@app/useEntityRegistry';

const SelectInput = styled(Select)`
    width: 100%;
`;

const StyleTag = styled(Tag)`
    padding: 0px 7px 0px 0px;
    margin-right: 3px;
    display: flex;
    justify-content: start;
    align-items: center;
`;

const LoadingWrapper = styled.div`
    padding: 8px;
    display: flex;
    justify-content: center;

    svg {
        height: 15px;
        width: 15px;
        color: ${ANTD_GRAY[8]};
    }
`;

type Props = {
    selectedUrns: string[];
    onChange: (urns: string[]) => void;
    mode?: 'multiple' | 'tags';
    placeholder?: string;
};

export const OrganizationPicker = ({
    selectedUrns,
    onChange,
    mode = 'multiple',
    placeholder = 'Search for organizations...',
}: Props) => {
    const entityRegistry = useEntityRegistry();
    const [orgSearch, { data: orgSearchData, loading: searchLoading }] = useGetSearchResultsLazyQuery();
    const searchResults = orgSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];
    const inputEl = useRef(null);

    const handleSearch = (text: string) => {
        orgSearch({
            variables: {
                input: {
                    type: EntityType.Organization,
                    query: text,
                    start: 0,
                    count: 10,
                },
            },
        });
    };

    const renderSearchResult = (entity: Entity) => {
        const displayName = entityRegistry.getDisplayName(entity.type, entity);
        return (
            <Select.Option value={entity.urn} key={entity.urn}>
                <OwnerLabel name={displayName} type={entity.type} />
            </Select.Option>
        );
    };

    const options = searchResults.map((result) => renderSearchResult(result));

    const onSelect = (urn: string) => {
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
        const newUrns = [...selectedUrns, urn];
        onChange(newUrns);
    };

    const onDeselect = (urn: string) => {
        const newUrns = selectedUrns.filter((u) => u !== urn);
        onChange(newUrns);
    };

    const tagRender = (props) => {
        // eslint-disable-next-line react/prop-types
        const { label, closable, onClose } = props;
        const onPreventMouseDown = (event) => {
            event.preventDefault();
            event.stopPropagation();
        };
        return (
            <StyleTag onMouseDown={onPreventMouseDown} closable={closable} onClose={onClose}>
                {label}
            </StyleTag>
        );
    };

    function handleBlur() {
        // Clear search on blur
    }

    return (
        <SelectInput
            mode={mode}
            ref={inputEl}
            placeholder={placeholder}
            showSearch
            filterOption={false}
            defaultActiveFirstOption={false}
            onSelect={(urn: any) => onSelect(urn)}
            onDeselect={(urn: any) => onDeselect(urn)}
            onSearch={(value: string) => {
                handleSearch(value.trim());
            }}
            tagRender={tagRender}
            onBlur={handleBlur}
            value={selectedUrns}
            notFoundContent={
                !searchLoading ? (
                    <Empty
                        description="No Organizations Found"
                        image={Empty.PRESENTED_IMAGE_SIMPLE}
                        style={{ color: ANTD_GRAY[7] }}
                    />
                ) : null
            }
        >
            {searchLoading ? (
                <Select.Option value="loading" key="loading">
                    <LoadingWrapper>
                        <LoadingOutlined />
                    </LoadingWrapper>
                </Select.Option>
            ) : (
                options
            )}
        </SelectInput>
    );
};
