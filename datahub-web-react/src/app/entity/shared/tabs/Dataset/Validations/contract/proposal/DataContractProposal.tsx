import React, { useEffect } from 'react';
import styled from 'styled-components';
import { Button, message, Modal, Typography } from 'antd';
import {
    CheckOutlined,
    CloseOutlined,
    EyeOutlined,
    InfoCircleOutlined,
    PlusOutlined,
    StopOutlined,
} from '@ant-design/icons';
import { useEntityData } from '../../../../../EntityContext';
// import { useGetContractProposalsQuery } from '../../../../../../../../graphql/contract.generated';
// import {
//     ActionRequestStatus,
//     ActionRequestType,
//     DataContractProposalParams,
//     EntityType,
// } from '../../../../../../../../types.generated';
import { DataContractProposalDescription } from './DataContractProposalDescription';
// import {
//     useAcceptProposalMutation,
//     useRejectProposalMutation,
// } from '../../../../../../../../graphql/actionRequest.generated';
import { ANTD_GRAY } from '../../../../../constants';
import { FAILURE_COLOR_HEX } from '../../../../Incident/incidentUtils';
import { FreshnessContractSummary } from '../FreshnessContractSummary';
import { SchemaContractSummary } from '../SchemaContractSummary';
import { DataQualityContractSummary } from '../DataQualityContractSummary';
import analytics, { EntityActionType, EventType } from '../../../../../../../analytics';

const Container = styled.div``;

const Summary = styled.div`
    width: 100%;
    padding-left: 40px;
    padding-top: 20px;
    padding-bottom: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    box-shadow: 0px 2px 6px 0px #0000000d;
`;

const SummaryDescription = styled.div`
    display: flex;
    align-items: center;
`;

const SummaryMessage = styled.div`
    display: inline-block;
    margin-left: 20px;
    max-width: 350px;
`;

const SummaryTitle = styled(Typography.Title)`
    && {
        padding-bottom: 0px;
        margin-bottom: 4px;
    }
`;

const Actions = styled.div`
    margin: 12px;
    margin-right: 20px;
`;

const ApproveButton = styled(Button)`
    margin-right: 12px;
    background-color: ${(props) => props.theme.styles['primary-color']};
    border-color: ${(props) => props.theme.styles['primary-color']};
    color: white;
    letter-spacing: 2px;
`;

const CreateButton = styled(Button)`
    margin-right: 12px;
    border-color: ${(props) => props.theme.styles['primary-color']};
    color: ${(props) => props.theme.styles['primary-color']};
    letter-spacing: 2px;
    &&:hover {
        color: white;
        background-color: ${(props) => props.theme.styles['primary-color']};
        border-color: ${(props) => props.theme.styles['primary-color']};
    }
`;

const DenyButton = styled(Button)`
    color: ${FAILURE_COLOR_HEX};
    border-color: ${FAILURE_COLOR_HEX};
    letter-spacing: 2px;
    &&:hover {
        color: white;
        background-color: ${FAILURE_COLOR_HEX};
        border-color: ${FAILURE_COLOR_HEX};
    }
    margin-right: 12px;
`;

const StyledInfoCircleOutlined = styled(InfoCircleOutlined)`
    margin-left: 4px;
    font-size: 10px;
    color: ${ANTD_GRAY[7]};
`;

const StyledEyeOutlined = styled(EyeOutlined)`
    font-size: 24px;
    color: ${ANTD_GRAY[7]};
`;

// type Props = {
//     showContractBuilder: () => void;
//     refetch: () => void;
//     entityUrn: string;
//     entityType?: EntityType;
// };

/**
 *  Displaying a Data Contract proposal for an entity.
 */

export const DataContractProposal = ({ showContractBuilder, refetch, entityUrn, entityType }: any) => {
    const { urn } = useEntityData();
    // const [acceptProposalMutation] = useAcceptProposalMutation();
    // const [rejectProposalMutation] = useRejectProposalMutation();
    // const { data, refetch: contractRefetch } = useGetContractProposalsQuery({
    //     variables: {
    //         urn,
    //     },
    // });
    const data: any = {
        listActionRequests: {
            total: 0,
            actionRequests: [],
            __typename: 'ListActionRequestsResult',
        },
    };
    const contractRefetch = () => null;

    useEffect(() => {
        contractRefetch();
    }, [contractRefetch]);

    const acceptProposal = (actionRequestUrn) => {
        Modal.confirm({
            title: 'Accept Proposed Contract',
            content: 'Are you sure you want to accept this proposal? New assertions will be created for this dataset.',
            okText: 'Yes',
            onOk() {
                // acceptProposalMutation({ variables: { urn: actionRequestUrn } })
                //     .then(() => {
                //         analytics.event({
                //             type: EventType.EntityActionEvent,
                //             actionType: 'ProposalAccepted', //EntityActionType.ProposalAccepted,
                //             actionQualifier: 'DATA_CONTRACT', //ActionRequestType.DataContract,
                //             entityType,
                //             entityUrn,
                //         });
                //         setTimeout(() => refetch(), 3000);
                //         message.success('Successfully accepted the proposal!');
                //     })
                //     .catch((err) => {
                //         console.log(err);
                //         message.error('Failed to accept proposal. :(');
                //     });
            },
        });
    };

    const rejectProposal = (actionRequestUrn) => {
        Modal.confirm({
            title: 'Reject Proposed Contract',
            content:
                'Are you sure you want to reject this proposal? Proposals will no longer be created for this dataset.',
            okText: 'Yes',
            onOk() {
                // rejectProposalMutation({ variables: { urn: actionRequestUrn } })
                //     .then(() => {
                //         analytics.event({
                //             type: EventType.EntityActionEvent,
                //             actionType: 'ProposalRejected', //EntityActionType.ProposalRejected,
                //             actionQualifier: 'DATA_CONTRACT', // ActionRequestType.DataContract,
                //             entityType,
                //             entityUrn,
                //         });
                //         contractRefetch();
                //         setTimeout(() => refetch(), 3000);
                //         message.success('Rejected the proposal.');
                //     })
                //     .catch((err) => {
                //         console.log(err);
                //         message.error('Failed to reject proposal. :(');
                //     });
            },
        });
    };

    // Extract the Contract which is being proposed. Note that this only goes to the current user if they are able to approve.
    const hasContractProposal =
        ((data?.listActionRequests?.total || 0) > 0 && data?.listActionRequests?.actionRequests?.length) || undefined;
    const contractActionRequest = hasContractProposal && data?.listActionRequests?.actionRequests[0];
    const actionRequestStatus = contractActionRequest && contractActionRequest.status;
    const actionRequestParams = contractActionRequest && contractActionRequest.params;
    const actionRequestUrn = contractActionRequest && contractActionRequest.urn;
    const contractProposal = actionRequestParams && actionRequestParams?.dataContractProposal;
    const isActiveProposal = contractProposal && actionRequestStatus === 'PENDING'; //ActionRequestStatus.Pending;

    return (
        <Container>
            <Summary>
                <SummaryDescription>
                    {(isActiveProposal && <StyledEyeOutlined />) || (
                        <StopOutlined style={{ color: ANTD_GRAY[6], fontSize: 24 }} />
                    )}
                    <SummaryMessage>
                        {(isActiveProposal && (
                            <>
                                <SummaryTitle level={5}>
                                    There is a contract proposal pending review <StyledInfoCircleOutlined />
                                </SummaryTitle>
                                <DataContractProposalDescription
                                    urn={urn}
                                    proposal={contractProposal} // as DataContractProposalParams}
                                />
                            </>
                        )) || (
                            <SummaryTitle level={5}>
                                No contract found
                                <div>
                                    <Typography.Text type="secondary">
                                        A contract does not yet exist for this dataset
                                    </Typography.Text>
                                </div>
                            </SummaryTitle>
                        )}
                    </SummaryMessage>
                </SummaryDescription>
                {(isActiveProposal && (
                    <Actions>
                        <ApproveButton onClick={() => acceptProposal(actionRequestUrn as any)}>
                            <CheckOutlined />
                            ACCEPT
                        </ApproveButton>
                        <DenyButton onClick={() => rejectProposal(actionRequestUrn as any)}>
                            <CloseOutlined />
                            REJECT
                        </DenyButton>
                        <CreateButton onClick={showContractBuilder}>
                            <PlusOutlined />
                            CREATE
                        </CreateButton>
                    </Actions>
                )) || (
                    <Actions>
                        <CreateButton onClick={showContractBuilder}>
                            <PlusOutlined />
                            CREATE
                        </CreateButton>
                    </Actions>
                )}
            </Summary>
            {isActiveProposal && contractProposal && contractProposal?.freshness && (
                <FreshnessContractSummary contracts={contractProposal?.freshness as any} />
            )}
            {isActiveProposal && contractProposal && contractProposal?.schema && (
                <SchemaContractSummary contracts={contractProposal?.schema as any} />
            )}
            {isActiveProposal && contractProposal && contractProposal?.dataQuality && (
                <DataQualityContractSummary contracts={contractProposal?.dataQuality as any} />
            )}
        </Container>
    );
};
