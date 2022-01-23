import React from 'react';
import { Button, Form, Select, Space } from 'antd';
import { gql, useQuery } from '@apollo/client';
import { useBaseEntity } from '../../../EntityContext';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';

export const EditSamples = () => {
    const queryTimeStamps = gql`
        query getProfiles($urn: String!) {
            dataset(urn: $urn) {
                datasetProfiles(limit: 10) {
                    timestampMillis
                }
            }
        }
    `;
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const currDataset = baseEntity && baseEntity?.dataset?.urn;
    const { data } = useQuery(queryTimeStamps, {
        variables: {
            urn: currDataset,
        },
        skip: currDataset === undefined,
    });
    const { Option } = Select;
    const timeStampValues = data?.dataset?.datasetProfiles || [];
    const refined = timeStampValues.map((item) => {
        return new Intl.DateTimeFormat('en-US', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
        }).format(item.timestampMillis);
    });
    console.log(`array is ${refined}`);
    return (
        <>
            <Form.Item name="chooseSet" label="Select a Dataset Profile to edit">
                <Select
                    defaultValue="select a timeperiod"
                    style={{ width: 300 }}
                    onChange={(value) => {
                        console.log(`option has changed to ${value}!`);
                    }}
                >
                    {refined.map((item) => (
                        <Option value={item}>{item}</Option>
                    ))}
                </Select>
                <Space />
                <Button>Load Profile</Button>
                <Button>Delete Profile</Button>
                <Button>Create New Dataset Profile</Button>
            </Form.Item>
        </>
    );
};
