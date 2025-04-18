import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';

import CreatedByView from '@app/actionrequest/item/CreatedByView';
import { DataContractProposalModal } from '@app/entity/shared/tabs/Dataset/Validations/contract/proposal/DataContractProposalModal';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ActionRequest, EntityType } from '@types';

interface Props {
    actionRequest: ActionRequest;
}

export default function CreateDataContractContentView({ actionRequest }: Props) {
    const [showDetails, setShowDetails] = useState(false);
    const entityRegistry = useEntityRegistry();
    const dataset = actionRequest?.entity;
    const dataContractParams = actionRequest?.params?.dataContractProposal;
    return (
        <span>
            <CreatedByView actionRequest={actionRequest} />
            <Typography.Text>
                {' '}
                requests to update <b>Data Contract</b> for Dataset{' '}
                <Link to={`${entityRegistry.getEntityUrl(EntityType.Dataset, dataset?.urn as string)}`}>
                    <Typography.Text strong>
                        {entityRegistry.getDisplayName(EntityType.Dataset, dataset)}
                    </Typography.Text>
                </Link>
                <Button type="link" onClick={() => setShowDetails(true)}>
                    View details
                </Button>
            </Typography.Text>
            {showDetails && dataContractParams && dataset && (
                <DataContractProposalModal
                    proposal={dataContractParams}
                    onClose={() => setShowDetails(false)}
                    showActions={false}
                />
            )}
        </span>
    );
}
