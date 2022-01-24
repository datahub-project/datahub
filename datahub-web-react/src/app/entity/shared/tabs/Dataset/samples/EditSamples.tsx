import React, { useState } from 'react';
import { Button, Form, Select, Space } from 'antd';
import { gql, useQuery } from '@apollo/client';
import { useBaseEntity } from '../../../EntityContext';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
// import axios from 'axios';

function GetProfileTimestamps(datasetUrn) {
    const queryTimeStamps = gql`
        query getProfiles($urn: String!) {
            dataset(urn: $urn) {
                datasetProfiles(limit: 15) {
                    timestampMillis
                }
            }
        }
    `;
    const { data } = useQuery(queryTimeStamps, {
        variables: {
            urn: datasetUrn,
        },
        skip: datasetUrn === undefined,
    });
    const timeStampValues = data?.dataset?.datasetProfiles || [];
    return timeStampValues;
}

function GetSpecificProfile(datasetUrn, inputtimestamp){
    const queryTimeStamps = gql`
        query getProfiles($urn: String!, $timestamp: Long!) {
            dataset(urn: $urn) {
                datasetProfiles(limit: 1, endTimeMillis: $timestamp) {
                    fieldProfiles {
                        fieldPath
                        sampleValues
                    }
                }
            }
        }
    `;
    const { data } = useQuery(queryTimeStamps, {
        variables: {
            urn: datasetUrn,
            timestamp: inputtimestamp,
        },
        skip: datasetUrn === undefined, 
    });
    const timeStampValues = data?.dataset?.datasetProfiles?.fieldProfiles || [];
    return timeStampValues;
}

export const EditSamples = () => {
    // const queryTimeStamps = gql`
    //     query getProfiles($urn: String!) {
    //         dataset(urn: $urn) {
    //             datasetProfiles(limit: 15) {
    //                 timestampMillis
    //             }
    //         }
    //     }
    // `;

    // const queryProfile = gql`
    //     query getProfiles($urn: String!, $timestamp: Long!) {
    //         dataset(urn: $urn) {
    //             datasetProfiles(limit: 1, startTimeMillis: $timestamp) {
    //                 fieldProfiles {
    //                     fieldPath
    //                 }
    //             }
    //         }
    //     }
    // `;

    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const [selectedValue, setSelectedValue] = useState<string>('');
    const currDataset = baseEntity && baseEntity?.dataset?.urn;
    // const { data } = useQuery(queryTimeStamps, {
    //     variables: {
    //         urn: currDataset,
    //     },
    //     skip: currDataset === undefined,
    // });
    const { Option } = Select;
    const timeStampValues = GetProfileTimestamps(currDataset);
    const refined = timeStampValues.map((item) => {
        return item.timestampMillis;
    });
    const deleteProfile = () => {
        console.log(`delete profile ${selectedValue}`);
        // axios delete profile endpoint
    };
    const loadProfile = () => {
        console.log(`Load profile ${selectedValue}`);
    };
    return (
        <>
            <Form.Item name="chooseSet" label="Select a Timestamped Dataset Profile to edit">
                <Select
                    defaultValue="select a timeperiod"
                    style={{ width: 300 }}
                    onChange={(value) => {
                        setSelectedValue(value);
                    }}
                >
                    {refined.map((item) => (
                        <Option value={item} key={item}>
                            {new Intl.DateTimeFormat('en-US', {
                                year: 'numeric',
                                month: '2-digit',
                                day: '2-digit',
                                hour: '2-digit',
                                minute: '2-digit',
                                second: '2-digit',
                            }).format(item)}
                        </Option>
                    ))}
                </Select>
                <Space />
                <Button onClick={loadProfile}>Load Profile</Button>
                <Button onClick={deleteProfile}>Delete Profile</Button>
                <Button>Create New Dataset Profile</Button>
            </Form.Item>
        </>
    );
};
