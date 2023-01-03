// import { Empty } from 'antd';
import React, { useEffect, useState } from 'react';
import { gql, useQuery } from '@apollo/client';
import { Button, Form } from 'antd';
import axios from 'axios';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useBaseEntity } from '../../../EntityContext';
import { SpecifyBrowsePath } from '../../../../../create/Components/SpecifyBrowsePath';
// import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import { FindMyUrn, FindWhoAmI, GetMyToken } from '../../../../dataset/whoAmI';
import { WhereAmI } from '../../../../../home/whereAmI';
import { printErrorMsg, printSuccessMsg } from '../ApiCallUtils';
// import adhocConfig from '../../../../../../conf/Adhoc';

function computeFinal(input) {
    const dataPaths = input?.browsePaths.map((x) => {
        const temp: [] = x.path;
        // as of 0.8.45, no longer need to pop the last element.
        // however, need to update paths for legacy datasets or it looks weird
        // temp.splice(temp.length - 1);
        // console.log(temp);
        return `/${temp.join('/')}/`;
        // return '/'+`${temp.join('/')}`+'/'
    });
    const formatted = dataPaths?.map((x) => {
        return x;
    });
    return formatted || [''];
}

export const EditBrowsePathTable = () => {
    const urlBase = WhereAmI();
    const publishUrl = `${urlBase}custom/update_browsepath`;
    console.log(`the final url is ${publishUrl}`);
    const [originalData, setOriginalData] = useState();
    const [modifiedState, setModifiedState] = useState(false);
    const layout = {
        labelCol: {
            span: 6,
        },
        wrapperCol: {
            span: 14,
        },
    };
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const currUrn = baseEntity && baseEntity.dataset && baseEntity.dataset?.urn;
    const currUser = FindWhoAmI();
    const currUserUrn = FindMyUrn();
    const userToken = GetMyToken(currUserUrn);
    // console.log(`user is ${currUser} and token is ${userToken}, received at ${Date().toLocaleString()}`);
    // console.log(currUrn);
    const [form] = Form.useForm();
    const queryresult = gql`
        {
            browsePaths(
                input: {
                    urn: "${currUrn}"
                    type: DATASET
                }
            ) {
                path
            }
        }
    `;
    const { data } = useQuery(queryresult, { skip: currUrn === undefined });
    const onFinish = async (values) => {
        axios
            .post(publishUrl, {
                dataset_name: currUrn,
                requestor: currUser,
                browsePaths: values.browsepathList,
                user_token: userToken,
            })
            .then((response) => {
                printSuccessMsg(response.status);
                setModifiedState(false);
                window.location.reload();
            })
            .catch((error) => {
                printErrorMsg(error.toString());
            });
    };
    const onReset = () => {
        form.resetFields();
        setModifiedState(false);
        form.setFieldsValue({
            browsepathList: (originalData || []).map((x) => {
                return {
                    browsepath: x,
                };
            }),
        });
    };
    console.log(`the browsepath to show is ${originalData}`);
    const handleFormChange = () => {
        const hasErrors = form.getFieldsError().some(({ errors }) => errors.length);
        setModifiedState(!hasErrors);
    };
    useEffect(() => {
        const formatted = computeFinal(data);
        setOriginalData(formatted);
        form.resetFields();
        form.setFieldsValue({
            browsepathList: formatted.map((x) => {
                return {
                    browsepath: x,
                };
            }),
        });
    }, [form, data]);
    return (
        <Form name="dynamic_item" {...layout} form={form} onFinish={onFinish} onFieldsChange={handleFormChange}>
            <Button type="primary" htmlType="submit" disabled={!modifiedState}>
                Submit
            </Button>
            &nbsp;
            <Button htmlType="button" onClick={onReset}>
                Reset
            </Button>
            <SpecifyBrowsePath />
        </Form>
    );
};
