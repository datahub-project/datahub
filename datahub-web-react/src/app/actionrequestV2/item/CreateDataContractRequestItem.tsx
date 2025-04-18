import { colors, Text } from '@components';
import { DataContractProposalModal } from '@src/app/entity/shared/tabs/Dataset/Validations/contract/proposal/DataContractProposalModal';
import { ActionRequest } from '@src/types.generated';
import React, { useState } from 'react';
import { Swap } from 'phosphor-react';
import CreatedByView from './CreatedByView';
import { ContentWrapper } from './styledComponents';
import RequestTargetEntityView from './RequestTargetEntityView';
import { ViewDocumentationButton } from './updateDescription/UpdateDescriptionRequestItem';

interface Props {
    actionRequest: ActionRequest;
}

export default function CreateDataContractRequestItem({ actionRequest }: Props) {
    const [showDetails, setShowDetails] = useState(false);
    const dataset = actionRequest?.entity;
    const dataContractParams = actionRequest?.params?.dataContractProposal;

    return (
        <ContentWrapper>
            <CreatedByView actionRequest={actionRequest} />
            <Text color="gray" type="span" weight="medium">
                {' '}
                requests to update <b>Data Contract</b> for Dataset{' '}
            </Text>
            <RequestTargetEntityView actionRequest={actionRequest} />
            {'  '}
            <ViewDocumentationButton variant="text" size="sm" onClick={() => setShowDetails(true)}>
                <Swap size={16} color={colors.gray[500]} />
                <Text color="gray"> View details</Text>
            </ViewDocumentationButton>
            {showDetails && dataContractParams && dataset && (
                <DataContractProposalModal
                    proposal={dataContractParams}
                    onClose={() => setShowDetails(false)}
                    showActions={false}
                />
            )}
        </ContentWrapper>
    );
}
