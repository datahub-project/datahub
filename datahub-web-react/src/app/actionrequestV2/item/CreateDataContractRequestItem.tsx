import { Text, colors } from '@components';
import { Swap } from 'phosphor-react';
import React, { useState } from 'react';

import CreatedByView from '@app/actionrequestV2/item/CreatedByView';
import RequestTargetEntityView from '@app/actionrequestV2/item/RequestTargetEntityView';
import { ContentWrapper } from '@app/actionrequestV2/item/styledComponents';
import { ViewDocumentationButton } from '@app/actionrequestV2/item/updateDescription/UpdateDescriptionRequestItem';
import { DataContractProposalModal } from '@src/app/entity/shared/tabs/Dataset/Validations/contract/proposal/DataContractProposalModal';
import { ActionRequest } from '@src/types.generated';

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
