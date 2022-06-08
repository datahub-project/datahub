import axios from 'axios';
import React, { useState } from 'react';
import { Button, Form, Input } from 'antd';
import { FieldStringOutlined } from '@ant-design/icons/';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { WhereAmI } from '../../../../../home/whereAmI';
import { FindMyUrn, FindWhoAmI, GetMyToken } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { printErrorMsg, printSuccessMsg } from '../ApiCallUtils';

export const EditDisplayName = () => {
    const urlBase = WhereAmI();
    const updateUrl = `${urlBase}custom/update_dataset_name`;
    const currUser = FindWhoAmI();
    const userUrn = FindMyUrn();
    const userToken = GetMyToken(userUrn);
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const currUrn = baseEntity && baseEntity.dataset && baseEntity.dataset?.urn;
    const existingProperties = useBaseEntity<GetDatasetQuery>()?.dataset?.properties;
    const [modifiedState, setModifiedState] = useState(false);
    const [formState] = Form.useForm();
    const layout = {
        labelCol: {
            span: 6,
        },
        wrapperCol: {
            span: 14,
        },
    };
    const resetForm = () => {
        setModifiedState(false);
        formState.resetFields();
    };

    const updateForm = () => {
        if (existingProperties?.name !== undefined) {
            if (formState.getFieldValue('displayName') !== '') {
                setModifiedState(true);
            } else {
                setModifiedState(false);
            }
        } else if (formState.getFieldValue('displayName') !== existingProperties?.name) {
            setModifiedState(true);
        } else {
            setModifiedState(false);
        }
    };
    const submitForm = async (values) => {
        const submission = {
            dataset_name: currUrn,
            requestor: currUser,
            displayName: values.displayName,
            user_token: userToken,
        };
        axios
            .post(updateUrl, submission)
            .then((response) => {
                printSuccessMsg(response.status);
                setModifiedState(false);
                window.location.reload();
            })
            .catch((error) => {
                printErrorMsg(error.toString());
            });
    };
    return (
        <>
            <Form
                {...layout}
                initialValues={{
                    displayName: existingProperties?.name || '',
                }}
                form={formState}
                onValuesChange={updateForm}
                onFinish={submitForm}
            >
                <Button type="primary" htmlType="submit" disabled={!modifiedState}>
                    Submit
                </Button>
                &nbsp;
                <Button htmlType="button" onClick={resetForm}>
                    Reset
                </Button>
                <Form.Item
                    name="displayName"
                    label="Rename Dataset"
                    rules={[
                        {
                            required: false,
                        },
                    ]}
                >
                    <Input placeholder="Insert Display Name" prefix={<FieldStringOutlined />} />
                </Form.Item>
            </Form>
        </>
    );
};
