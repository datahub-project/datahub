import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { Button, Form, message, Select } from 'antd';
import { gql, useLazyQuery, useQuery } from '@apollo/client';
import { useBaseEntity } from '../../../EntityContext';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import { FindMyUrn, FindWhoAmI, GetMyToken } from '../../../../dataset/whoAmI';
import adhocConfig from '../../../../../../conf/Adhoc';

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
function timeout(delay: number) {
    return new Promise((res) => setTimeout(res, delay));
}

export const EditSampleForm = () => {
    const baseUrl = adhocConfig;
    const branch = baseUrl.lastIndexOf('/');
    const makeUrl = `${baseUrl.substring(0, branch)}/update_samples`;
    const delUrl = `${baseUrl.substring(0, branch)}/delete_samples`;
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
    const user = useGetAuthenticatedUser();
    const userUrn = FindMyUrn();
    const currUser = FindWhoAmI();
    const userToken = GetMyToken(userUrn);
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

    const createNewProfile = () => {
        getSchema();
        setToggle(true);
        setSelectedValue('');
        setHasSelectedDate(false);
        sethasModifiedForm(false);
    };
    const printSuccessMsg = (status) => {
        message.success(`Status:${status} - Request submitted successfully`, 3).then();
    };
    const printErrorMsg = (error) => {
        message.error(error, 3).then();
    };
    const handleValuesChange = (value, key: string) => {
        const copyFormData: any = { ...formData };
        copyFormData[key] = value;
        setFormData(copyFormData);
        sethasModifiedForm(true);
    };
    const deleteProfile = async () => {
        const deleteSubmission = {
            requestor: userUrn,
            user_token: userToken,
            timestamp: selectedValue,
            dataset_name: currDataset,
        };
        // console.log(`data to be submitted is ${JSON.stringify(deleteSubmission)}`);
        axios
            .post(delUrl, deleteSubmission)
            .then((response) => printSuccessMsg(response.status))
            .catch((error) => {
                printErrorMsg(error.toString());
            });
        await timeout(3000);
        window.location.reload();
    };
    const submitData = async () => {
        const formTimestamp = toggle ? Date.now() : Number(selectedValue);
        const createSubmission = {
            user_token: userToken,
            requestor: currUser,
            dataset_name: currDataset,
            samples: formData,
            timestamp: formTimestamp,
        };        
        axios
            .post(makeUrl, createSubmission)
            .then((response) => printSuccessMsg(response.status))
            .catch((error) => {
                printErrorMsg(error.toString());
            });
        await timeout(3000);
        window.location.reload();
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
                    data-testid="selectprofileoption"
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
            <p>
                Up to <b>3</b> sample values will be shown in UI for each field.
            </p>
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
