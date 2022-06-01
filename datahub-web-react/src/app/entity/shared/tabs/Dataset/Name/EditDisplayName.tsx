import axios from 'axios';
import React, { useState } from 'react';
import { Button, Form, Input, message } from 'antd';
import { FieldStringOutlined } from '@ant-design/icons/';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { WhereAmI } from '../../../../../home/whereAmI';
import { FindMyUrn, FindWhoAmI, GetMyToken } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';

function timeout(delay: number) {
    return new Promise((res) => setTimeout(res, delay));
}

export const EditDisplayName = () => {
    const urlBase = WhereAmI();
    const updateUrl = `${urlBase}custom/update_dataset_name`;
    const currUser = FindWhoAmI();
    const userUrn = FindMyUrn();
    const userToken = GetMyToken(userUrn);
    const datasetUrn = useBaseEntity<GetDatasetQuery>()?.dataset?.urn;
    const existingProperties = useBaseEntity<GetDatasetQuery>()?.dataset?.properties;
    const [modifiedState, setModifiedState] = useState(true);
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
        setModifiedState(true);
        formState.resetFields();
    };
    const printSuccessMsg = async (status) => {
        message.success(`Status:${status} - Request submitted successfully`, 3).then();
        await timeout(3000);
        window.location.reload();
    };
    const printErrorMsg = (error) => {
        message.error(error, 3).then();
    };

    const updateForm = () => {
        const hasBeenModified = formState.getFieldValue('displayName') === (existingProperties?.name || '');
        setModifiedState(hasBeenModified);
    };
    const submitForm = async (values) => {
        const submission = {
            dataset_name: datasetUrn,
            requestor: currUser,
            displayName: values.displayName,
            user_token: userToken,
        };
        axios
            .post(updateUrl, submission)
            .then((response) => printSuccessMsg(response.status))
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
                <Button type="primary" htmlType="submit" disabled={modifiedState}>
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
