// import { Empty } from 'antd';
import React, { useEffect, useState } from 'react';
import { gql, useQuery } from '@apollo/client';
import { Button, Form, message } from 'antd';
import axios from 'axios';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useBaseEntity } from '../../../EntityContext';
import { SpecifyBrowsePath } from '../../../../../create/Components/SpecifyBrowsePath';
// import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import { FindMyUrn, FindWhoAmI, GetMyToken } from '../../../../dataset/whoAmI';
// import adhocConfig from '../../../../../../conf/Adhoc';

function computeFinal(input) {
    const dataPaths = input?.browsePaths.map((x) => {
        const temp: [] = x.path;
        temp.splice(temp.length - 1);
        // console.log(temp);
        return `/${temp.join('/')}/`;
        // return '/'+`${temp.join('/')}`+'/'
    });
    const formatted = dataPaths?.map((x) => {
        return x;
    });
    return formatted || [''];
}
function timeout(delay: number) {
    return new Promise((res) => setTimeout(res, delay));
}

export const EditBrowsePathTable = () => {
    const initialUrl = window.location.href;
    // this wacky setup is because the URL is different when running docker-compose vs Ingress
    // for docker-compose, need to change port. For ingress, just modify subpath will do.
    // having a setup that works for both makes development easier.
    // for UI edit pages, the URL is complicated, need to find the root path.
    const mainPathLength = initialUrl.split('/', 3).join('/').length;
    const mainPath = `${initialUrl.substring(0, mainPathLength + 1)}`;
    let publishUrl = initialUrl.includes(':3000') ? initialUrl.replace(':3000/', ':8001/custom/update_browsepath') : mainPath;
    publishUrl = publishUrl.includes(':9002') 
        ? publishUrl.replace(':9002/', ':8001/custom/update_browsepath') 
        : `${mainPath}/custom/update_browsepath`
    console.log(`the final url is ${publishUrl}`);
    // let url = adhocConfig;
    // const branch = url.lastIndexOf('/');
    // url = `${url.substring(0, branch)}/update_browsepath`;
    // console.log(`eventual url is ${url}`);
    const [originalData, setOriginalData] = useState();
    const [disabledSave, setDisabledSave] = useState(true);
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
    // const currDataset = useBaseEntity<GetDatasetOwnersGqlQuery>()?.dataset?.urn;
    // const currUser = useGetAuthenticatedUser()?.corpUser?.username || '-';
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
    const printSuccessMsg = (status) => {
        message.success(`Status:${status} - Request submitted successfully`, 3).then();
    };
    const printErrorMsg = (error) => {
        message.error(error, 3).then();
    };
    const onFinish = async (values) => {
        axios
            .post(publishUrl, {
                dataset_name: currUrn,
                requestor: currUser,
                browsePaths: values.browsepathList,
                user_token: userToken,
            })
            .then((response) => printSuccessMsg(response.status))
            .catch((error) => {
                printErrorMsg(error.toString());
            });
        await timeout(3000);
        window.location.reload();
    };
    const onReset = () => {
        form.resetFields();
        form.setFieldsValue({
            browsepathList: originalData,
        });
    };
    const handleFormChange = () => {
        const hasErrors = form.getFieldsError().some(({ errors }) => errors.length);
        setDisabledSave(hasErrors);
    };
    useEffect(() => {
        const formatted = computeFinal(data);
        setOriginalData(formatted);
        form.resetFields();
        form.setFieldsValue({
            browsepathList: formatted,
        });
    }, [form, data]);
    return (
        <Form name="dynamic_item" {...layout} form={form} onFinish={onFinish} onFieldsChange={handleFormChange}>
            <Button type="primary" htmlType="submit" disabled={disabledSave}>
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
