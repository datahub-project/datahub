import React, { useState } from 'react';
import { Button, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { ActionRequest, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import CreatedByView from './CreatedByView';
import { DataContractProposalModal } from '../../entity/shared/tabs/Dataset/Validations/contract/proposal/DataContractProposalModal';

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
