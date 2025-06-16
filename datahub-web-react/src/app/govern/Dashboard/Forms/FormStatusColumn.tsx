import { useApolloClient } from '@apollo/client';
import { Pill, Tooltip } from '@components';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { updateFormAssignmentStatusInList } from '@app/govern/Dashboard/Forms/cacheUtils';
import { getStatusDetails, isFormAssignmentInProgress } from '@app/govern/Dashboard/Forms/formUtils';

import { useGetFormAssignmentStatusLazyQuery } from '@graphql/form.generated';
import { AssignmentStatus, FormAssignmentStatus, FormInfo, FormSettings, FormState } from '@types';

const StatusContainer = styled.div`
    display: inherit;
`;

const NotifyInfo = styled.div`
    margin-top: 12px;
`;

interface Props {
    formUrn: string;
    formInfo?: FormInfo;
    formAssignmentStatus?: FormAssignmentStatus;
    formSettings?: FormSettings;
}

export default function FormStatusColumn({ formUrn, formInfo, formAssignmentStatus, formSettings }: Props) {
    const client = useApolloClient();
    const formStatus = formInfo?.status?.state || FormState.Draft;
    const [assignmentStatus, setAssignmentStatus] = useState<FormAssignmentStatus | undefined | null>(
        formAssignmentStatus,
    );

    const [getFormAssignmentStatus] = useGetFormAssignmentStatusLazyQuery({
        onCompleted: (data) => {
            if (
                data.form?.formAssignmentStatus?.status === AssignmentStatus.InProgress &&
                formStatus === FormState.Published
            ) {
                setTimeout(() => getFormAssignmentStatus({ variables: { urn: formUrn } }), 5000);
            }
            // update cache with actual status
            updateFormAssignmentStatusInList(client, formUrn, data.form?.formAssignmentStatus);
            setAssignmentStatus(data.form?.formAssignmentStatus);
        },
    });

    const isInProgress = isFormAssignmentInProgress(formStatus, assignmentStatus);
    const willNotify = formSettings?.notificationSettings?.notifyAssigneesOnPublish;

    useEffect(() => {
        if (isInProgress) {
            setTimeout(() => getFormAssignmentStatus({ variables: { urn: formUrn } }), 3000);
        }
    }, [isInProgress, formUrn, getFormAssignmentStatus]);

    const { label, colorScheme } = getStatusDetails(formStatus, assignmentStatus);
    return (
        <Tooltip
            title={
                isInProgress ? (
                    <>
                        We are assigning this form to your assets. This may take a while.
                        {willNotify && <NotifyInfo>Assignees will be notified once assignment is complete.</NotifyInfo>}
                    </>
                ) : undefined
            }
        >
            <StatusContainer data-testid={`${formUrn}-status-${formStatus.toLowerCase()}`}>
                <Pill label={label} color={colorScheme} clickable={false} />
            </StatusContainer>
        </Tooltip>
    );
}
