import React, { useEffect, useState } from 'react';
import { Button, Form, Select } from 'antd';
import { gql, useLazyQuery, useQuery } from '@apollo/client';
import { useBaseEntity } from '../../../EntityContext';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
// import { width } from '../../../../../lineage/LineageEntityNode';

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
    const formItemLayout = {
        labelCol: { span: 6 },
        wrapperCol: { span: 14 },
    };
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const currDataset = baseEntity && baseEntity?.dataset?.urn;
    const [selectedValue, setSelectedValue] = useState(0);
    const [modifiedForm, setModifiedForm] = useState(false);
    const [formData, setFormData] = useState({});
    const [profileData, setprofileData] = useState({});
    const [toggle, setToggle] = useState(true); // true = create,false = load. default true
    const [getProfile, { data: profiledata }] = useLazyQuery(queryTimeStamps, {
        variables: {
            urn: currDataset,
            timestamp: selectedValue,
        },
    });
    const [getSchema, { data: schemaData }] = useLazyQuery(querySchema, {
        variables: {
            urn: currDataset,
        },
    });
    const [schema, setSchema] = useState([]);
    useEffect(() => {
        setSchema(schemaData?.dataset?.schemaMetadata?.fields.map((item) => item.fieldPath) || []);
    }, [schemaData]);
    useEffect(() => {
        return toggle
            ? setFormData(
                  Object.fromEntries(
                      schema.map((item) => {
                          return [item, []];
                      }) || {},
                  ),
              )
            : setFormData(profileData);
    }, [schema, toggle, profileData]);

    useEffect(() => {
        setprofileData(
            profiledata?.dataset?.datasetProfiles?.[0].fieldProfiles.reduce(
                (obj, item) => ({ ...obj, [item.fieldPath]: item.sampleValues }),
                {},
            ) || {},
        );
    }, [profiledata]);
    const { Option } = Select;
    const timeStampValues = GetProfileTimestamps(currDataset).map((item) => {
        return item.timestampMillis;
    });
    const deleteProfile = () => {
        console.log(`delete profile ${selectedValue}`);
    };

    const loadProfile = () => {
        getProfile();
        setToggle(false);
        // setFormData(
        //     profiledata?.dataset?.datasetProfiles?.[0].fieldProfiles.reduce(
        //         (obj, item) => ({ ...obj, [item.fieldPath]: item.sampleValues }),
        //         {},
        //     ) || {},
        // );
    };

    const createNewProfile = () => {
        getSchema();
        setToggle(true);
    };

    const handleChange = (value, key: string) => {
        console.log(`key is ${key}`);
        const copyFormData: any = { ...formData };
        copyFormData[key] = value;
        setFormData(copyFormData);
    };
    const submitData = () => {
        console.log(`data to be submitted is ${JSON.stringify(formData)}`);
    };
    return (
        <>
            <Form.Item name="chooseSet" label="Select a existing Dataset Profile to Edit">
                <Select
                    placeholder="select a timeperiod"
                    style={{ width: 200 }}
                    onChange={(value) => {
                        setSelectedValue(Number(value));
                        setModifiedForm(true);
                    }}
                >
                    {timeStampValues.map((item) => (
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
                <Button onClick={loadProfile} disabled={!modifiedForm} key="load">
                    Load Profile
                </Button>
                <Button onClick={deleteProfile} key="delete">
                    Delete Profile
                </Button>
                <Button onClick={createNewProfile} key="create">
                    Create New Profile
                </Button>
                <Button onClick={submitData} key="submit">
                    Submit Changes
                </Button>
            </Form.Item>
            {Object.keys(formData).map((mykey) => (
                <Form.Item label={mykey} {...formItemLayout}>
                    <Select
                        id={mykey}
                        mode="tags"
                        style={{ float: 'right', width: '100%' }}
                        tokenSeparators={['|']}
                        value={formData[mykey]}
                        key={mykey}
                        onChange={(e) => handleChange(e, mykey)}
                    />
                </Form.Item>
            ))}
        </>
    );
};
