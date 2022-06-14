import React, { useState } from 'react';
// import { Select } from 'antd';
import Select from 'antd/lib/select';
import styled from 'styled-components';
import { Col, Form, Popover, Row } from 'antd';
import { SetParentContainer } from '../containerEdit/SetParentContainer';

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
    'urn:li:dataPlatform:file',
    'urn:li:dataPlatform:mongodb',
    'urn:li:dataPlatform:elasticsearch',
    'urn:li:dataPlatform:mssql',
    'urn:li:dataPlatform:mysql',
    'urn:li:dataPlatform:oracle',
    'urn:li:dataPlatform:mariadb',
    'urn:li:dataPlatform:hdfs',
    'urn:li:dataPlatform:hive',
    'urn:li:dataPlatform:kudu',
    'urn:li:dataPlatform:postgres',
    'urn:li:dataPlatform:openapi',
];

export const DataPlatformSelect = () => {
    // Unfortunately there is no way to query for available platforms.
    // Hence, must resort to fixed list.
    // in order to seed the dropdown if the user immediately click on the drop down,
    // i will initialise selectedPlaform with FILE platform.
    const [selectedPlatform, setSelectedPlatform] = useState('urn:li:dataPlatform:hive');
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
    const aboutPlatform =
        'If dataset is not from an existing database, use FILE. For databases not in list, refer to admin';
    const onSelectMember = (urn: string) => {
        setSelectedPlatform(urn);
    };
    console.log(`selected platform: ${selectedPlatform}`);
    const removeOption = () => {
        console.log(`removing ${selectedPlatform}`);
        setSelectedPlatform('');
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
                <Row>
                    <Col span={8} offset={0}>
                        <Popover trigger="hover" content={aboutPlatform}>
                            <Select
                                autoFocus
                                filterOption
                                showSearch
                                value={selectedPlatform}
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
                        </Popover>
                    </Col>
                </Row>
            </Form.Item>
            <SetParentContainer platformType={selectedPlatform} compulsory={false} />
        </>
    );
};
