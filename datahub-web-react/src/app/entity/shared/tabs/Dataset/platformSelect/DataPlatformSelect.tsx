import React, { useState } from 'react';
// import { Select } from 'antd';
import Select from 'antd/lib/select';
import styled from 'styled-components';
import { Form } from 'antd';
import { useGetSearchResultsQuery } from '../../../../../../graphql/search.generated';
import { EntityType, SearchResult } from '../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';

const SearchResultContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 2px;
`;

const SearchResultContent = styled.div`
    display: flex;
    justify-content: start;
    align-items: center;
`;

const SearchResultDisplayName = styled.div`
    margin-left: 5px;
`;

const platformSelection = [
    'urn:li:dataPlatform:csv',
    // 'urn:li:dataPlatform:mongodb',
    // 'urn:li:dataPlatform:elasticsearch',
    // 'urn:li:dataPlatform:mssql',
    'urn:li:dataPlatform:mysql',
    // 'urn:li:dataPlatform:oracle',
    // 'urn:li:dataPlatform:mariadb',
    // 'urn:li:dataPlatform:hdfs',
    'urn:li:dataPlatform:hive',
    'urn:li:dataPlatform:kudu',
    'urn:li:dataPlatform:postgres',
];

export const DataPlatformSelect = () => {
    // Unfortunately there is no way to query for available platforms.
    // Hence, must resort to fixed list.
    // in order to seed the dropdown if the user immediately click on the drop down,
    // i will initialise selectedPlaform with FILE platform.
    const [selectedPlatform, setSelectedPlatform] = useState('');
    const [selectedContainers, setSelectedContainers] = useState('');
    console.log(`parentcontainer is ${selectedContainers}`);
    const { data: containerCandidates } = useGetSearchResultsQuery({
        variables: {
            input: {
                type: EntityType.Container,
                query: '*',
                filters: [
                    {
                        field: 'platform',
                        value: selectedPlatform,
                    },
                ],
            },
        },
    });
    const candidates = containerCandidates?.search?.searchResults || [];
    const entityRegistry = useEntityRegistry();
    const renderSearchedContainer = (result: SearchResult) => {
        const displayName = entityRegistry.getDisplayName(result.entity.type, result.entity);
        return displayName;
    };
    const renderSearchResult = (result: string) => {
        const displayName = result.split(':').pop()?.toUpperCase();
        return (
            <SearchResultContainer>
                <SearchResultContent>
                    <SearchResultDisplayName>
                        <div>{displayName}</div>
                    </SearchResultDisplayName>
                </SearchResultContent>
            </SearchResultContainer>
        );
    };
    // const aboutPlatform =
    //     'If dataset is not from an existing database, use FILE. For databases not in list, refer to admin';
    const onSelectMember = (urn: string) => {
        setSelectedPlatform(urn);
        setSelectedContainers('');
    };
    console.log(`selected platform: ${selectedPlatform}`);
    const removeOption = () => {
        console.log(`removing ${selectedPlatform}`);
        setSelectedPlatform('');
        setSelectedContainers('');
    };
    return (
        <>
            <Form.Item
                name="platformSelect"
                label="Specify a Data Source Type"
                rules={[
                    {
                        required: true,
                        message: 'A type MUST be specified.',
                    },
                ]}
            >
                <Select
                    value={selectedPlatform}
                    defaultValue=""
                    showArrow
                    placeholder="Search for a parent container.."
                    onSelect={(platform: string) => onSelectMember(platform)}
                    allowClear
                    onClear={removeOption}
                    onDeselect={removeOption}
                >
                    {platformSelection?.map((platform) => (
                        <Select.Option value={platform}>{renderSearchResult(platform)}</Select.Option>
                    ))}
                </Select>
            </Form.Item>
            <Form.Item
                // {...formItemLayout}
                name="parentContainer"
                label="Specify a Container(Optional)"
                rules={[
                    {
                        required: false,
                        message: 'A container must be specified.',
                    },
                ]}
                shouldUpdate={(prevValues, curValues) => prevValues.platformSelect !== curValues.platformSelect}
            >
                <Select
                    filterOption
                    value={selectedContainers}
                    showArrow
                    placeholder="Search for a parent container.."
                    allowClear
                    onSelect={(container: any) => setSelectedContainers(container)}
                >
                    {candidates.map((result) => (
                        <Select.Option key={result?.entity?.urn} value={result?.entity?.urn}>
                            {renderSearchedContainer(result)}
                        </Select.Option>
                    ))}
                </Select>
            </Form.Item>
        </>
    );
};
