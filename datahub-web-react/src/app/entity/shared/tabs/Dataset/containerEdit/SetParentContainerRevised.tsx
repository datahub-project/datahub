import React, { useEffect, useMemo, useState } from 'react';
import { Col, Row, Select, Form } from 'antd';
import PropTypes from 'prop-types';
import { useGetContainerLazyQuery } from '../../../../../../graphql/container.generated';
import { useGetSearchResultsLazyQuery } from '../../../../../../graphql/search.generated';
import { EntityType, SearchResult } from '../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';

interface ContainerValue {
    platformType?: string;
    platformContainer?: string;
}

interface ContainerInputProps {
    value?: ContainerValue;
    onChange?: (value: ContainerValue) => void;
}

export const SetParentContainerRevised: React.FC<ContainerInputProps> = ({ value = {} }) => {
    const entityRegistry = useEntityRegistry();
    const [containerPath, setContainerPath] = useState(<div />);
    const [selectedContainerUrn, setSelectedContainerUrn] = useState(value?.platformContainer || '');
    // lazyquery to fire the query whenever we need it based on the event
    const [runContainerQuery, { data: containerData }] = useGetContainerLazyQuery();
    const [runQuery, { data: containerCandidates }] = useGetSearchResultsLazyQuery({
        variables: {
            input: {
                type: EntityType.Container,
                query: '*',
                filters: [
                    {
                        field: 'platform',
                        value: value?.platformType || '',
                    },
                ],
                count: 1000,
            },
        },
    });

    useEffect(() => {
        runQuery();
    }, [runQuery]);

    useEffect(() => {
        // reset the selected container urn whenever user selects a new platform type in another select field
        if (value?.platformContainer !== undefined && value?.platformContainer === '') {
            setSelectedContainerUrn('');
            setContainerPath(<div />);
        }
    }, [value?.platformContainer, value?.platformType]);
    //  runs when one of its dependencies update to recalculate the container path
    useMemo(() => {
        const selectedName = containerData?.container?.properties?.name || '';
        const containersArray = containerData?.container?.parentContainers?.containers || [];
        const parentArray = containersArray?.map((item) => item?.properties?.name) || [];
        const pathString = parentArray.reverse().join(' => ') || '';
        const outputString1 =
            pathString === '' ? `No parent container to this container ` : `Parent containers to this container: `;
        const outputString2a = pathString === '' ? '' : `${pathString} => `;
        const outputString2b = selectedName === '' ? `` : `${selectedName} (selected)`;
        const finalString2 = (
            <div className="displayContainerPath">
                {outputString1}
                <span style={{ color: 'blue' }}>{outputString2a}</span>
                <span style={{ color: 'red' }}>{outputString2b}</span>
            </div>
        );
        setContainerPath(finalString2);
    }, [containerData?.container?.parentContainers?.containers, containerData?.container?.properties?.name]);

    const containerPool = containerCandidates?.search?.searchResults || [];

    const renderSearchResult = (result: SearchResult) => {
        const displayName = entityRegistry.getDisplayName(result.entity.type, result.entity);
        return displayName;
    };

    const handleChange = (temp) => {
        runContainerQuery({
            variables: {
                urn: temp,
            },
        });
    };

    return (
        <Row>
            <Col span={6}>
                <Form.Item name="parentContainer" style={{ marginBottom: '0px' }}>
                    <Select
                        value={selectedContainerUrn}
                        onSelect={(container: any) => {
                            setSelectedContainerUrn(container);
                        }}
                        filterOption
                        showArrow
                        placeholder="Search for a parent container.."
                        onChange={handleChange}
                    >
                        {containerPool.map((result) => (
                            <Select.Option key={result?.entity?.urn} value={result?.entity?.urn}>
                                {renderSearchResult(result)}
                            </Select.Option>
                        ))}
                    </Select>
                </Form.Item>
            </Col>
            &nbsp;
            <Col>{containerPath}</Col>
        </Row>
    );
};

SetParentContainerRevised.propTypes = {
    value: PropTypes.shape({
        platformType: PropTypes.string.isRequired,
        platformContainer: PropTypes.string.isRequired,
    }),
};
