// import { Empty } from 'antd';
import React from 'react';
import { Button, message, Popconfirm, Result } from 'antd';
import axios from 'axios';
// import { gql, useQuery } from '@apollo/client';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
// import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import { useBaseEntity } from '../../../EntityContext';
import { FindMyUrn, FindWhoAmI, GetMyToken } from '../../../../dataset/whoAmI';
// import adhocConfig from '../../../../../../conf/Adhoc';

// function CheckStatus(queryresult, currDataset) {
//     const { data } = useQuery(queryresult, { skip: currDataset === undefined });
//     const currStatus = data === undefined ? false : data;
//     return currStatus;
// }
function timeout(delay: number) {
    return new Promise((res) => setTimeout(res, delay));
}

function CheckStatus(entity) {
    const rawStatus = entity?.dataset?.status?.removed;
    const currStatus = rawStatus === undefined ? false : rawStatus;
    return currStatus;
}

export const DeleteSchemaTabv2 = () => {
    // const entity = useBaseEntity<GetDatasetQuery>();
    // const delay = (ms) => new Promise((res) => setTimeout(res, ms));
    const initialUrl = window.location.href;
    // this wacky setup is because the URL is different when running docker-compose vs Ingress
    // for docker-compose, need to change port. For ingress, just modify subpath will do.
    // having a setup that works for both makes development easier.
    // for UI edit pages, the URL is complicated, need to find the root path.
    const mainPathLength = initialUrl.split('/', 3).join('/').length;
    const mainPath = `${initialUrl.substring(0, mainPathLength + 1)}`;
    const publishUrl = mainPath.includes(':3000')
        ? mainPath.replace(':3000/', ':8001/custom/update_dataset_status')
        : `${mainPath}/custom/update_dataset_status`;
    console.log(`the final url is ${publishUrl}`);
    // let url = adhocConfig;
    // const branch = url.lastIndexOf('/');
    // url = `${url.substring(0, branch)}/update_dataset_status`;
    const [visible, setVisible] = React.useState(false);
    const [confirmLoading, setConfirmLoading] = React.useState(false);
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const currDataset = baseEntity && baseEntity?.dataset?.urn;
    const currStatusBase = baseEntity && baseEntity?.dataset?.status?.removed;
    const currStatus = currStatusBase === undefined ? false : currStatusBase;
    console.log(`the current status of the dataset is removed:, ${currStatusBase}, ${currStatus}`);
    const statusFinal = currStatus ? 'error' : 'success';
    const statusMsg = currStatus ? 'Dataset is not searchable' : 'Dataset is searchable via search and listing';
    const buttonMsg = currStatus ? 'Activate Dataset' : 'Deactivate Dataset';
    const popupMsg = `Confirm ${buttonMsg}`;

    const warning =
        "You wouldn't be able to find this page after navigating away. Please copy page url before leaving page in case you need to undo deactivation.";
    const subMsg = currStatus ? warning : '';

    // const currUser = useGetAuthenticatedUser()?.corpUser?.username || '-';
    const currUser = FindWhoAmI();
    const currUserUrn = FindMyUrn();
    const userToken = GetMyToken(currUserUrn);
    // console.log(`user is ${currUser} and token is ${userToken}, received at ${Date().toLocaleString()}`);
    const printSuccessMsg = (status) => {
        message.success(`Status:${status} - Request submitted successfully`, 3).then();
    };
    const printErrorMsg = (errorMsg) => {
        message.error(errorMsg, 3).then();
    };

    const deleteDataset = async () => {
        axios
            .post(publishUrl, {
                dataset_name: currDataset,
                requestor: currUser,
                desired_state: !CheckStatus(baseEntity),
                user_token: userToken,
            })
            .then((response) => printSuccessMsg(response.status))
            .catch((exception) => {
                printErrorMsg(exception.toString());
            });
        await timeout(3000);
        window.location.reload();
    };

    const showPopconfirm = () => {
        setVisible(true);
    };
    const handleOk = () => {
        setConfirmLoading(true);
        deleteDataset();
        setTimeout(() => {
            setVisible(false);
            setConfirmLoading(false);
        }, 3000);
    };

    const handleCancel = () => {
        // console.log('Clicked cancel button');
        setVisible(false);
    };
    return (
        <>
            <Result
                status={statusFinal}
                title={statusMsg}
                subTitle={subMsg}
                extra={[
                    <Popconfirm
                        title={popupMsg}
                        visible={visible}
                        onConfirm={handleOk}
                        okButtonProps={{ loading: confirmLoading }}
                        onCancel={handleCancel}
                    >
                        <Button type="primary" key="console" onClick={showPopconfirm}>
                            {buttonMsg}
                        </Button>
                        ,
                    </Popconfirm>,
                ]}
            />
            ,
        </>
    );
};
