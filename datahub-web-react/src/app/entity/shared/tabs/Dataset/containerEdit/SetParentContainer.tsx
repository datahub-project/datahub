import React, { useEffect, useState } from 'react';
import Select from 'antd/lib/select';
import { Form, Tooltip } from 'antd';
import { EntityType, SearchResult } from '../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useGetSearchResultsQuery } from '../../../../../../graphql/search.generated';

interface Props {
    platformType: string;
    compulsory: boolean;
}

export const SetParentContainer = (props: Props) => {
    // need this to render the display name of the container
    // decided not to put name of parent container of selected container - the new feature in 0.8.36 would be better
    const aboutContainer = 'Select a collection that this dataset belongs to. Can be optional';
    const [selectedContainers, setSelectedContainers] = useState('');
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
            },
        },
    });
    const entityRegistry = useEntityRegistry();
    const renderSearchResult = (result: SearchResult) => {
        const displayName = entityRegistry.getDisplayName(result.entity.type, result.entity);
        return displayName;
    };
    const candidates = containerCandidates?.search?.searchResults || [];
    // 'Container represents a physical collection of dataset. To create a new container, refer to admin';
    return (
        <>
            {/* <Popover trigger="hover" content={aboutContainer}> */}
            <Tooltip title={aboutContainer}>
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
                        onSelect={(container: any) => setSelectedContainers(container)}
                        style={{ width: '20%' }}
                    >
                        {candidates.map((result) => (
                            <Select.Option key={result?.entity?.urn} value={result?.entity?.urn}>
                                {renderSearchResult(result)}
                            </Select.Option>
                        ))}
                    </Select>
                </Form.Item>
            </Tooltip>
        </>
    );
};
