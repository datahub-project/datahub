// import { Empty } from 'antd';
import React from 'react';
import { Button, message, Popconfirm, Result } from 'antd';
import axios from 'axios';
import { gql, useQuery } from '@apollo/client';
import { GetDatasetOwnersGqlQuery } from '../../../../../../graphql/dataset.generated';
import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import { useBaseEntity } from '../../../EntityContext';

function CheckStatus(queryresult, currDataset) {
    const { data } = useQuery(queryresult, { skip: currDataset === undefined });
    const currStatus = data === undefined ? false : data;
    return currStatus;
}

function timeout(delay: number) {
    return new Promise((res) => setTimeout(res, delay));
}
export const DeleteSchemaTabv2 = () => {
    // const entity = useBaseEntity<GetDatasetQuery>();
    // const delay = (ms) => new Promise((res) => setTimeout(res, ms));

    const [visible, setVisible] = React.useState(false);
    const [confirmLoading, setConfirmLoading] = React.useState(false);
    const currDataset = useBaseEntity<GetDatasetOwnersGqlQuery>()?.dataset?.urn;
    const queryresult = gql`
        {
            dataset(urn: "${currDataset}") {
                status {
                    removed
                }
            }
        }
    `;
    const { data } = useQuery(queryresult, { skip: currDataset === undefined });

    const currStatus = data === undefined ? false : data;
    console.log(`the current status of the dataset is removed:"${data}`);
    const statusFinal = currStatus ? 'error' : 'success';
    const statusMsg = currStatus ? 'Dataset is not searchable' : 'Dataset is searchable via search and listing';
    const buttonMsg = currStatus ? 'Activate Dataset' : 'Deactivate Dataset';
    const popupMsg = `Confirm ${buttonMsg}`;
    const warning =
        "You wouldn't be able to find this page after navigating away. Please copy page url before leaving page in case you need to undo deactivation.";
    const subMsg = currStatus ? warning : '';

    const currUser = useGetAuthenticatedUser()?.corpUser?.username || '-';
    const printSuccessMsg = (status) => {
        message.success(`Status:${status} - Request submitted successfully`, 3).then();
    };
    const printErrorMsg = (error) => {
        message.error(error, 3).then();
    };

    const deleteDataset = async () => {
        axios
            .post('http://localhost:8001/update_dataset_status', {
                dataset_name: currDataset,
                requestor: currUser,
                desired_state: !CheckStatus(queryresult, currDataset),
            })
            .then((response) => printSuccessMsg(response.status))
            .catch((error) => {
                printErrorMsg(error.toString());
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
        console.log('Clicked cancel button');
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
