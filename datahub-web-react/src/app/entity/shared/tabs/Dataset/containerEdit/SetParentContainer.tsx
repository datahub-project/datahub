import React, { useState } from 'react';
import Select from 'antd/lib/select';
import { Form } from 'antd';
import { EntityType, SearchResult } from '../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useGetSearchResultsQuery } from '../../../../../../graphql/search.generated';

interface Props {
    platformType: string;
    compulsory: boolean;
}

const formItemLayout = {
    labelCol: { span: 6 },
    wrapperCol: { span: 14 },
};

export const SetParentContainer = (props: Props) => {
    // need this to render the display name of the container
    // decided not to put name of parent container of selected container - the new feature in 0.8.36 would be better
    const [selectedContainers, setSelectedContainers] = useState('');
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

    return (
        <>
            <Form.Item
                {...formItemLayout}
                name="parentContainer"
                label="Specify a Container for the Dataset (Optional)"
                rules={[
                    {
                        required: props.compulsory,
                        message: 'A container must be specified.',
                    },
                ]}
            >
                <Select
                    style={{ width: 300 }}
                    autoFocus
                    filterOption
                    value={selectedContainers}
                    showArrow
                    placeholder="Search for a parent container.."
                    allowClear
                    onSelect={(container: any) => setSelectedContainers(container)}
                >
                    {containerCandidates?.search?.searchResults.map((result) => (
                        <Select.Option value={result?.entity?.urn}>{renderSearchResult(result)}</Select.Option>
                    ))}
                </Select>
            </Form.Item>
        </>
    );
};
