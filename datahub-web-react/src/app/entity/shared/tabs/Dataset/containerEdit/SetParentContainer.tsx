import React, { useEffect, useState } from 'react';
// import { gql, useLazyQuery } from '@apollo/client';
import Select from 'antd/lib/select';
import { Col, Form } from 'antd';
import { EntityType, SearchResult } from '../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useGetSearchResultsQuery } from '../../../../../../graphql/search.generated';
import { useGetContainerQuery } from '../../../../../../graphql/container.generated';

interface Props {
    platformType: string;
    compulsory: boolean;
    clear: boolean;
}

export const SetParentContainer = (props: Props) => {
    // need this to render the display name of the container
    // decided not to put name of parent container of selected container - the new feature in 0.8.36 would be better
    // const aboutContainer = 'Select a collection that this dataset belongs to. Can be optional';
    const [selectedContainers, setSelectedContainers] = useState('');
    const [candidatePool, setCandidatePool] = useState<any>([]);
    const [erasePath, setErasePath] = useState(false);
    const [parentPath, setParentPath] = useState('');
    console.log(`erase is set to ${erasePath}`);
    useEffect(() => {
        setErasePath(props.clear);
    }, [props.clear]);

    // const [parentPath, setParentPath] = useState('');
    useEffect(() => {
        setSelectedContainers('');
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
            urn: selectedContainers,
        },
        skip: selectedContainers === '',
    });
    // const queryParentPath = gql`
    //     query parentPath($urn: String!) {
    //         container(urn: $urn) {
    //             parentContainers {
    //                 containers {
    //                     ... on Container {
    //                         properties {
    //                             name
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // `;

    useEffect(() => {
        const containersArray = containerData?.container?.parentContainers?.containers || [];
        const pathArray = containersArray?.map((item) => item?.properties?.name) || [];
        const outputString = pathArray.join(' => ') || '';
        setParentPath(outputString);
    }, [containerData]);

    // const [queryParent] = useLazyQuery(queryParentPath, {
    //     onCompleted(data) {
    //         const generatedPath = derivedPath(data);
    //         setParentPath(generatedPath);
    //     },
    // });
    const entityRegistry = useEntityRegistry();
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
                name="parentContainer"
                label="Specify a Container(Optional)"
                rules={[
                    {
                        required: props.compulsory,
                        message: 'A container must be specified.',
                    },
                ]}
                shouldUpdate={(prevValues, curValues) => prevValues.props !== curValues.props}
            >
                <Select
                    filterOption
                    value={selectedContainers}
                    showArrow
                    placeholder="Search for a parent container.."
                    allowClear
                    onSelect={(container: any) => {
                        setSelectedContainers(container);
                        changedSelect();
                    }}
                    onChange={changedSelect}
                    style={{ width: '20%' }}
                >
                    {candidatePool.map((result) => (
                        <Select.Option key={result?.entity?.urn} value={result?.entity?.urn}>
                            {renderSearchResult(result)}
                        </Select.Option>
                    ))}
                </Select>
            </Form.Item>
            <Col span="18" offset="6">
                {erasePath ? '' : parentPath}
            </Col>
        </>
    );
};
