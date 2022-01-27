import React, { useEffect, useState } from 'react';
import { Button, Form, Select } from 'antd';
import { gql, useLazyQuery, useQuery } from '@apollo/client';
import { useBaseEntity } from '../../../EntityContext';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';

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
const formItemLayout = {
    labelCol: { span: 6 },
    wrapperCol: { span: 14 },
};
// function timeout(delay: number) {
//     return new Promise((res) => setTimeout(res, delay));
// }

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
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const currDataset = baseEntity && baseEntity?.dataset?.urn;
    const [selectedValue, setSelectedValue] = useState('');
    const [hasSelectedDate, setHasSelectedDate] = useState(false);
    const [hasModifiedForm, sethasModifiedForm] = useState(false);
    const [formData, setFormData] = useState({});
    const [profileData, setprofileData] = useState({});
    const [schema, setSchema] = useState([]);
    const [toggle, setToggle] = useState(true); // true = create,false = load. default true
    const [getProfile, { data: profiledata }] = useLazyQuery(queryTimeStamps);
    const [getSchema, { data: schemaData }] = useLazyQuery(querySchema, {
        variables: {
            urn: currDataset,
        },
    });

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
        console.log(JSON.stringify(profiledata));
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
    const deleteProfile = async () => {
        console.log(`delete profile ${selectedValue}`);
        // await timeout(3000);
        // window.location.reload();
    };
    // const loadProfile = () => {
    //     getProfile();
    //     setToggle(false);
    // };

    const createNewProfile = () => {
        getSchema();
        setToggle(true);
        setSelectedValue('');
        setHasSelectedDate(false);
        sethasModifiedForm(false);
    };

    const handleValuesChange = (value, key: string) => {
        const copyFormData: any = { ...formData };
        copyFormData[key] = value;
        setFormData(copyFormData);
        sethasModifiedForm(true);
    };
    const submitData = async () => {
        const formTimestamp = toggle ? Date.now() : Number(selectedValue);
        console.log(`data to be submitted is ${JSON.stringify(formData)} for ${formTimestamp}`);
        // await timeout(3000);
        // window.location.reload();
    };
    const updateSelect = (value) => {
        setSelectedValue(value);
        setHasSelectedDate(true);
        sethasModifiedForm(false);
        setToggle(false);
        if (value !== '') {
            console.log(`I call you ${selectedValue} and value ${value}`);
            getProfile({
                variables: {
                    urn: currDataset,
                    timestamp: Number(value),
                },
            });
        }
    };
    return (
        <>
            <Form.Item name="chooseSet" label="Select an existing Dataset Profile">
                <Select
                    placeholder="select a timeperiod"
                    value={selectedValue}
                    style={{ width: 200 }}
                    onChange={updateSelect}
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
                {/* <Button onClick={loadProfile} disabled={!modifiedForm} key="load">
                    Load Profile
                </Button> */}
                <Button onClick={deleteProfile} disabled={!hasSelectedDate} key="delete">
                    Delete Profile
                </Button>
                <Button onClick={createNewProfile} key="create">
                    Create New Profile
                </Button>
                <Button onClick={submitData} disabled={!hasModifiedForm} key="submit">
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
                        onChange={(e) => handleValuesChange(e, mykey)}
                    />
                </Form.Item>
            ))}
        </>
    );
};
