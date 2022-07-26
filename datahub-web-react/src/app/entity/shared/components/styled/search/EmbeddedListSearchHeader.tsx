import React from 'react';
import { Button, Checkbox, Dropdown, Menu, Typography } from 'antd';
import { CaretDownOutlined, FilterOutlined } from '@ant-design/icons';
import type { CheckboxValueType } from 'antd/es/checkbox/Group';
import styled from 'styled-components';
import MenuItem from 'antd/lib/menu/MenuItem';

import TabToolbar from '../TabToolbar';
import { SearchBar } from '../../../../../search/SearchBar';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { EntityType, FacetFilterInput, SearchAcrossEntitiesInput } from '../../../../../../types.generated';
import { SearchResultsInterface } from './types';
import SearchExtendedMenu from './SearchExtendedMenu';
import { ANTD_GRAY } from '../../../constants';

const HeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
`;

const SearchAndDownloadContainer = styled.div`
    display: flex;
    align-items: center;
`;

const SearchMenuContainer = styled.div`
    margin-top: 7px;
    margin-left: 10px;
`;

const DownArrow = styled(CaretDownOutlined)`
    vertical-align: -1px;
    font-size: 8px;
    margin-left: 2px;
    margin-top: 2px;
    color: ${ANTD_GRAY[7]};
`;

const DropdownWrapper = styled.div`
    align-items: center;
    cursor: pointer;
    display: flex;
    margin-left: 12px;
    margin-right: 12px;
`;

type Props = {
    onSearch: (q: string) => void;
    onToggleFilters: () => void;
    placeholderText?: string | null;
    callSearchOnVariables: (variables: {
        input: SearchAcrossEntitiesInput;
    }) => Promise<SearchResultsInterface | null | undefined>;
    entityFilters: EntityType[];
    filters: FacetFilterInput[];
    query: string;
    setShowSelectMode: (showSelectMode: boolean) => any;
    showSelectMode: boolean;
    onToggleSelectAll: (selected: boolean) => void;
    checkedSearchResults: CheckboxValueType[];
};

export default function EmbeddedListSearchHeader({
    onSearch,
    onToggleFilters,
    placeholderText,
    callSearchOnVariables,
    entityFilters,
    filters,
    query,
    setShowSelectMode,
    showSelectMode,
    onToggleSelectAll,
    checkedSearchResults,
}: Props) {
    const entityRegistry = useEntityRegistry();

    const onQueryChange = (newQuery: string) => {
        onSearch(newQuery);
    };

    return (
        <TabToolbar>
            <HeaderContainer>
                {showSelectMode ? (
                    <>
                        <div style={{ display: 'flex', justifyContent: 'left', alignItems: 'center', marginLeft: 4 }}>
                            <Checkbox
                                onChange={(e) => onToggleSelectAll(e.target.checked as boolean)}
                                style={{ marginRight: 12, paddingBottom: 0 }}
                            />
                            <Typography.Text strong type="secondary">
                                {checkedSearchResults.length} selected
                            </Typography.Text>
                        </div>
                        <SearchAndDownloadContainer>
                            <Dropdown
                                trigger={['click']}
                                overlay={
                                    <Menu>
                                        <MenuItem key="0" style={{ padding: 0 }}>
                                            <Button
                                                style={{ fontWeight: 'normal' }}
                                                onClick={() => setShowSelectMode(false)}
                                                type="text"
                                            >
                                                Add owners
                                            </Button>
                                        </MenuItem>
                                        <MenuItem key="1" style={{ padding: 0 }}>
                                            <Button
                                                style={{ fontWeight: 'normal' }}
                                                onClick={() => setShowSelectMode(false)}
                                                type="text"
                                            >
                                                Remove owners
                                            </Button>
                                        </MenuItem>
                                    </Menu>
                                }
                            >
                                <DropdownWrapper>
                                    Owners
                                    <DownArrow />
                                </DropdownWrapper>
                            </Dropdown>
                            <Dropdown
                                trigger={['click']}
                                overlay={
                                    <Menu>
                                        <MenuItem key="0" style={{ padding: 0 }}>
                                            <Button
                                                style={{ fontWeight: 'normal' }}
                                                onClick={() => setShowSelectMode(false)}
                                                type="text"
                                            >
                                                Add Glossary Terms
                                            </Button>
                                        </MenuItem>
                                        <MenuItem key="1" style={{ padding: 0 }}>
                                            <Button
                                                style={{ fontWeight: 'normal' }}
                                                onClick={() => setShowSelectMode(false)}
                                                type="text"
                                            >
                                                Remove Glossary Terms
                                            </Button>
                                        </MenuItem>
                                    </Menu>
                                }
                            >
                                <DropdownWrapper>
                                    Glossary Terms
                                    <DownArrow />
                                </DropdownWrapper>
                            </Dropdown>
                            <Dropdown
                                trigger={['click']}
                                overlay={
                                    <Menu>
                                        <MenuItem key="0" style={{ padding: 0 }}>
                                            <Button
                                                style={{ fontWeight: 'normal' }}
                                                onClick={() => setShowSelectMode(false)}
                                                type="text"
                                            >
                                                Add Tags
                                            </Button>
                                        </MenuItem>
                                        <MenuItem key="1" style={{ padding: 0 }}>
                                            <Button
                                                style={{ fontWeight: 'normal' }}
                                                onClick={() => setShowSelectMode(false)}
                                                type="text"
                                            >
                                                Remove Tagss
                                            </Button>
                                        </MenuItem>
                                    </Menu>
                                }
                            >
                                <DropdownWrapper>
                                    Tags
                                    <DownArrow />
                                </DropdownWrapper>
                            </Dropdown>
                            <Dropdown
                                trigger={['click']}
                                overlay={
                                    <Menu>
                                        <MenuItem key="0" style={{ padding: 0 }}>
                                            <Button
                                                style={{ fontWeight: 'normal' }}
                                                onClick={() => setShowSelectMode(false)}
                                                type="text"
                                            >
                                                Set domain
                                            </Button>
                                        </MenuItem>
                                        <MenuItem key="1" style={{ padding: 0 }}>
                                            <Button
                                                style={{ fontWeight: 'normal' }}
                                                onClick={() => setShowSelectMode(false)}
                                                type="text"
                                            >
                                                Unset domain
                                            </Button>
                                        </MenuItem>
                                    </Menu>
                                }
                            >
                                <DropdownWrapper>
                                    Domain
                                    <DownArrow />
                                </DropdownWrapper>
                            </Dropdown>
                            <Dropdown
                                trigger={['click']}
                                overlay={
                                    <Menu>
                                        <MenuItem key="0" style={{ padding: 0 }}>
                                            <Button
                                                style={{ fontWeight: 'normal' }}
                                                onClick={() => setShowSelectMode(false)}
                                                type="text"
                                            >
                                                Mark as deprecated
                                            </Button>
                                        </MenuItem>
                                        <MenuItem key="1" style={{ padding: 0 }}>
                                            <Button
                                                style={{ fontWeight: 'normal' }}
                                                onClick={() => setShowSelectMode(false)}
                                                type="text"
                                            >
                                                Mark as undeprecated
                                            </Button>
                                        </MenuItem>
                                    </Menu>
                                }
                            >
                                <DropdownWrapper>
                                    Deprecation
                                    <DownArrow />
                                </DropdownWrapper>
                            </Dropdown>
                            <Dropdown
                                trigger={['click']}
                                overlay={
                                    <Menu>
                                        <MenuItem key="0" style={{ padding: 0 }}>
                                            <Button
                                                style={{ fontWeight: 'normal' }}
                                                onClick={() => setShowSelectMode(false)}
                                                type="text"
                                            >
                                                Mark as deleted
                                            </Button>
                                        </MenuItem>
                                        <MenuItem key="1" style={{ padding: 0 }}>
                                            <Button
                                                style={{ fontWeight: 'normal' }}
                                                onClick={() => setShowSelectMode(false)}
                                                type="text"
                                            >
                                                Mark as undeleted
                                            </Button>
                                        </MenuItem>
                                    </Menu>
                                }
                            >
                                <DropdownWrapper>
                                    Deletion
                                    <DownArrow />
                                </DropdownWrapper>
                            </Dropdown>
                            <Button
                                style={{
                                    marginLeft: '8px',
                                    padding: 0,
                                    color: ANTD_GRAY[6],
                                }}
                                onClick={() => setShowSelectMode(false)}
                                type="link"
                            >
                                Cancel
                            </Button>
                        </SearchAndDownloadContainer>
                    </>
                ) : (
                    <>
                        <Button type="text" onClick={onToggleFilters}>
                            <FilterOutlined />
                            <Typography.Text>Filters</Typography.Text>
                        </Button>
                        <SearchAndDownloadContainer>
                            <SearchBar
                                initialQuery=""
                                placeholderText={placeholderText || 'Search entities...'}
                                suggestions={[]}
                                style={{
                                    maxWidth: 220,
                                    padding: 0,
                                }}
                                inputStyle={{
                                    height: 32,
                                    fontSize: 12,
                                }}
                                onSearch={onSearch}
                                onQueryChange={onQueryChange}
                                entityRegistry={entityRegistry}
                            />
                            <SearchMenuContainer>
                                <SearchExtendedMenu
                                    callSearchOnVariables={callSearchOnVariables}
                                    entityFilters={entityFilters}
                                    filters={filters}
                                    query={query}
                                    setShowSelectMode={setShowSelectMode}
                                />
                            </SearchMenuContainer>
                        </SearchAndDownloadContainer>
                    </>
                )}
            </HeaderContainer>
        </TabToolbar>
    );
}
