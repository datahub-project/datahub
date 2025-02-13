import { Text } from '@components';
import { DataContractProposalModal } from '@src/app/entity/shared/tabs/Dataset/Validations/contract/proposal/DataContractProposalModal';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { ActionRequest, EntityType } from '@src/types.generated';
import { Button } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import CreatedByView from './CreatedByView';
import { ContentWrapper } from './styledComponents';

interface Props {
    actionRequest: ActionRequest;
}

export default function CreateDataContractRequestItem({ actionRequest }: Props) {
    const [showDetails, setShowDetails] = useState(false);
    const entityRegistry = useEntityRegistry();
    const dataset = actionRequest?.entity;
    const dataContractParams = actionRequest?.params?.dataContractProposal;

    return (
        <ContentWrapper>
            <CreatedByView actionRequest={actionRequest} />
            <Text color="gray" type="span" weight="medium">
                {' '}
                requests to update <b>Data Contract</b> for Dataset{' '}
                <Link to={`${entityRegistry.getEntityUrl(EntityType.Dataset, dataset?.urn as string)}`}>
                    <Text weight="bold">{entityRegistry.getDisplayName(EntityType.Dataset, dataset)}</Text>
                </Link>
                <Button type="link" onClick={() => setShowDetails(true)}>
                    View details
                </Button>
            </Text>
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
