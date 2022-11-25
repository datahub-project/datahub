import { Button, Form } from 'antd';
import axios from 'axios';
import React, { useState } from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { WhereAmI } from '../../../../../home/whereAmI';
import { FindMyUrn, FindWhoAmI, GetMyToken } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { printErrorMsg, printSuccessMsg } from '../ApiCallUtils';
import { SetParentContainer } from './SetParentContainer';

export const EditParentContainerPanel = () => {
    const urlBase = WhereAmI();
    const updateUrl = `${urlBase}custom/update_containers`;
    const userUrn = FindMyUrn();
    const currUser = FindWhoAmI();
    const userToken = GetMyToken(userUrn);
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const currUrn = baseEntity && baseEntity.dataset && baseEntity.dataset?.urn;
    const platform = baseEntity?.dataset?.platform?.urn || '';
    const containerValue = baseEntity?.dataset?.container?.properties?.name || 'none';
    const [clearParent, setClearParent] = useState(false);
    const [modifiedState, setModifiedState] = useState(false);

    const layout = {
        labelCol: {
            span: 6,
        },
        wrapperCol: {
            span: 14,
        },
    };
    const [formState] = Form.useForm();

    const updateForm = () => {
        setModifiedState(true);
        setClearParent(false);
    };
    const resetForm = () => {
        setModifiedState(false);
        setClearParent(true);
        formState.resetFields();
    };

    const onFinish = async (values) => {
        const proposedContainer = values.parentContainer;
        // container is always 1 only, hence list to singular value
        const submission = {
            dataset_name: currUrn,
            requestor: currUser,
            container: proposedContainer,
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
                name="dynamic_item"
                {...layout}
                form={formState}
                onFinish={onFinish}
                onValuesChange={updateForm}
                initialValues={{
                    parentContainer: containerValue,
                }}
            >
                <Button type="primary" htmlType="submit" disabled={!modifiedState}>
                    Submit
                </Button>
                &nbsp;
                <Button htmlType="button" onClick={resetForm}>
                    Reset
                </Button>
                <SetParentContainer platformType={platform} compulsory clear={clearParent} />
            </Form>
        </>
    );
};
