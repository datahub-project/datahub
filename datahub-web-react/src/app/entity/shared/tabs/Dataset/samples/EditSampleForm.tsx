import React, { useState } from 'react';
import { Button, Form, Select, Space } from 'antd';
import { gql, useLazyQuery, useQuery } from '@apollo/client';
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

function GetCurrentSchema(datasetUrn) {
    // to query the current schema in place so i can create an empty form with the existing schema
    const querySchema = gql`
        query getSchema($urn: String!) {
            dataset(urn: $urn) {
                schemaMetadata(version: 0) {
                    fields {
                        fieldPath
                    }
                }
            }
        }
    `;
    const { data: schema } = useQuery(querySchema, {
        variables: {
            urn: datasetUrn,
        },
        skip: datasetUrn === undefined,
    });

    const result = schema?.dataset?.schemaMetadata?.fields.map((item) => {
        return item.fieldPath;
    });
    return result;
}

export const EditSampleForm = () => {
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
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const currDataset = baseEntity && baseEntity?.dataset?.urn;
    const [selectedValue, setSelectedValue] = useState(0);
    const [formData, setFormData] = useState([]);
    const [getProfile, { data: profiledata }] = useLazyQuery(queryTimeStamps, {
        variables: {
            urn: currDataset,
            timestamp: selectedValue,
        },
    });

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
        console.log(`Load profile ${selectedValue} for ${currDataset}`);
        getProfile();
        setFormData(
            profiledata?.dataset?.datasetProfiles?.[0].fieldProfiles.map((field) => {
                return field;
            }) || {},
        );
    };
    const createNewProfile = () => {
        const labels = GetCurrentSchema(currDataset);
        console.log(labels);
    };
    // const outputData = profiledata?.dataset?.datasetProfiles || {};
    console.log(JSON.stringify(formData));
    return (
        <>
            <Form.Item name="chooseSet" label="Select a Timestamped Dataset Profile to edit">
                <Select
                    defaultValue="select a timeperiod"
                    style={{ width: 300 }}
                    onChange={(value) => {
                        setSelectedValue(Number(value));
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
                <Button onClick={createNewProfile}>Create New Dataset Profile</Button>
            </Form.Item>
        </>
    );
};
