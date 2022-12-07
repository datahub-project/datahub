import { Button, Form } from 'antd';
// import Select from 'antd/lib/select';
import axios from 'axios';
import React, { useState } from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { WhereAmI } from '../../../../../home/whereAmI';
import { FindMyUrn, FindWhoAmI, GetMyToken } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { printErrorMsg, printSuccessMsg } from '../ApiCallUtils';
import { SetParentContainerRevised } from './SetParentContainerRevised';
// import { useGetSearchResultsLazyQuery } from '../../../../../../graphql/search.generated';
// import { EntityType, SearchResult } from '../../../../../../types.generated';
// import { useGetContainerLazyQuery } from '../../../../../../graphql/container.generated';
// import { useEntityRegistry } from '../../../../../useEntityRegistry';

export const EditParentContainerPanel = () => {
    // const entityRegistry = useEntityRegistry();
    const urlBase = WhereAmI();
    const updateUrl = `${urlBase}custom/update_containers`;
    const userUrn = FindMyUrn();
    const currUser = FindWhoAmI();
    const userToken = GetMyToken(userUrn);
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const containerValue = baseEntity?.dataset?.container?.urn || '';
    const currUrn = baseEntity && baseEntity.dataset && baseEntity.dataset?.urn;
    const [disableSubmit, setDisableSubmit] = useState(true);
    const [platform] = useState(baseEntity?.dataset?.platform?.urn || '');

    const layout = {
        labelCol: {
            span: 6,
        },
        wrapperCol: {
            span: 14,
        },
    };
    const [formState] = Form.useForm();

    const resetForm = () => {
        formState.resetFields();
        setDisableSubmit(true);
    };
    const updateForm = (values) => {
        if (values.parentContainer !== containerValue && values.parentContainer !== '') {
            setDisableSubmit(false);
        }
        if (values.parentContainer === containerValue || values.parentContainer === '') {
            setDisableSubmit(true);
        }
    };
    const onFinish = async (values) => {
        console.log(values);
        const proposedContainer = values.parentContainer;
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
                    parentContainerProps: {
                        platformType: platform,
                        platformContainer: '',
                    },
                    parentContainer: containerValue,
                }}
            >
                <Button type="primary" htmlType="submit" disabled={disableSubmit}>
                    Submit
                </Button>
                &nbsp;
                <Button htmlType="button" onClick={resetForm}>
                    Reset
                </Button>
                <Form.Item name="parentContainerProps" label="Pick New Container for Dataset">
                    <SetParentContainerRevised />
                </Form.Item>
            </Form>
        </>
    );
};
