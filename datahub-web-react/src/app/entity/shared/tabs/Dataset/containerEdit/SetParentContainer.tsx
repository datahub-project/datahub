import React, { useEffect, useState } from 'react';
// import { gql, useLazyQuery } from '@apollo/client';
import Select from 'antd/lib/select';
import { Col, Form, Row } from 'antd';
// import styled from 'styled-components';
import { EntityType, SearchResult } from '../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useGetSearchResultsQuery } from '../../../../../../graphql/search.generated';
import { useGetContainerQuery } from '../../../../../../graphql/container.generated';

interface Props {
    platformType: string;
    compulsory: boolean;
    clear: boolean;
}
// const ParentContainerPath = styled.div`
//     background-color: 'blue';
// `;

export const SetParentContainer = (props: Props) => {
    // need this to render the display name of the container
    // decided not to put name of parent container of selected container - the new feature in 0.8.36 would be better
    // const aboutContainer = 'Select a collection that this dataset belongs to. Can be optional';
    const entityRegistry = useEntityRegistry();
    const [selectedContainerUrn, setSelectedContainerUrn] = useState('');
    const [candidatePool, setCandidatePool] = useState<any>([]);
    const [erasePath, setErasePath] = useState(false);
    const [parentPath, setParentPath] = useState<any>();
    useEffect(() => {
        setErasePath(props.clear);
        setParentPath(props.clear ? <div /> : parentPath);
        setCandidatePool(props.clear ? [] : candidatePool);
    }, [props.clear, candidatePool, parentPath]);
    // }, [props.clear, parentPath]);

    // const [parentPath, setParentPath] = useState('');
    useEffect(() => {
        setSelectedContainerUrn('');
    }, [props.platformType]);
    const { data: containerCandidates } = useGetSearchResultsQuery({
        variables: {
            input: {
                type: EntityType.Container,
                query: '*',
                filters: [
                    {
                        field: 'platform',
                        value: props.platformType,
                    },
                ],
                count: 1000,
            },
        },
    });
    const { data: containerData } = useGetContainerQuery({
        variables: {
            urn: selectedContainerUrn,
        },
        skip: selectedContainerUrn === '',
    });

    useEffect(() => {
        const selectedName = containerData?.container?.properties?.name;
        const containersArray = containerData?.container?.parentContainers?.containers || [];
        const parentArray = containersArray?.map((item) => item?.properties?.name) || [];
        const pathString = parentArray.reverse().join(' => ') || '';
        const outputString1 =
            pathString === '' ? `No parent container to this container ` : `Parent containers to this container: `;
        const outputString2a = pathString === '' ? '' : `${pathString} => `;
        const outputString2b = `${selectedName} (selected)`;
        const finalString2 = (
            <div className="blah">
                {outputString1}
                <span style={{ color: 'blue' }}>{outputString2a}</span>
                <span style={{ color: 'red' }}>{outputString2b}</span>
            </div>
        );
        const finalCheckString = selectedContainerUrn === '' ? '' : finalString2;
        setParentPath(finalCheckString);
    }, [containerData, selectedContainerUrn]);

    const renderSearchResult = (result: SearchResult) => {
        const displayName = entityRegistry.getDisplayName(result.entity.type, result.entity);
        return displayName;
    };

    useEffect(() => {
        setCandidatePool(containerCandidates?.search?.searchResults || []);
    }, [containerCandidates]);
    const changedSelect = () => {
        setErasePath(false);
    };
    // 'Container represents a physical collection of dataset. To create a new container, refer to admin';
    return (
        <>
            {/* <Popover trigger="hover" content={aboutContainer}> */}
            <Form.Item
                name="parentContainerRow"
                label="Specify a Container(Optional)"
                rules={[
                    {
                        required: props.compulsory,
                        message: 'A container must be specified.',
                    },
                ]}
                shouldUpdate={(prevValues, curValues) => prevValues.props !== curValues.props}
                style={{ marginBottom: '0px' }}
            >
                <Row>
                    <Col span={6}>
                        <Form.Item name="parentContainer">
                            <Select
                                filterOption
                                value={selectedContainerUrn}
                                showArrow
                                placeholder="Search for a parent container.."
                                allowClear
                                onSelect={(container: any) => {
                                    setSelectedContainerUrn(container);
                                    changedSelect();
                                }}
                                onChange={changedSelect}
                                // style={{ width: '20%' }}
                            >
                                {candidatePool.map((result) => (
                                    <Select.Option key={result?.entity?.urn} value={result?.entity?.urn}>
                                        {renderSearchResult(result)}
                                    </Select.Option>
                                ))}
                            </Select>
                        </Form.Item>
                    </Col>
                    &nbsp;
                    <Col span={12}>
                        <Form.Item name="parentContainer2">{erasePath ? '' : parentPath}</Form.Item>
                    </Col>
                </Row>
            </Form.Item>
        </>
    );
};
