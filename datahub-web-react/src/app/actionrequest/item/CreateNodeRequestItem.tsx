import React from 'react';
import { ActionRequest, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import CreateGlossaryEntityContentView from './CreateGlossaryEntityContentView';
import MetadataAssociationRequestItem from './MetadataAssociationRequestItem';

type Props = {
    actionRequest: ActionRequest;
    onUpdate: () => void;
    showActionsButtons: boolean;
};

const REQUEST_TYPE_DISPLAY_NAME = 'Create Term Group Proposal';

export default function CreateNodeRequestItem({ actionRequest, onUpdate, showActionsButtons }: Props) {
    const entityRegistry = useEntityRegistry();

    const proposedName = actionRequest.params?.createGlossaryNodeProposal?.glossaryNode.name || '';
    const parentNode = actionRequest.params?.createGlossaryNodeProposal?.glossaryNode.parentNode || null;
    const entityName = entityRegistry.getEntityName(EntityType.GlossaryNode);
    const contentView = (
        <CreateGlossaryEntityContentView
            proposedName={proposedName}
            actionRequest={actionRequest}
            entityName={entityName || ''}
            parentNode={parentNode}
        />
    );

    return (
        <MetadataAssociationRequestItem
            requestTypeDisplayName={REQUEST_TYPE_DISPLAY_NAME}
            requestContentView={contentView}
            actionRequest={actionRequest}
            onUpdate={onUpdate}
            showActionsButtons={showActionsButtons}
        />
    );
}
