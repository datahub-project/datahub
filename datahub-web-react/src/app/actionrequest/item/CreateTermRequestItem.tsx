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

const REQUEST_TYPE_DISPLAY_NAME = 'Create Glossary Term Proposal';

export default function CreateTermRequestItem({ actionRequest, onUpdate, showActionsButtons }: Props) {
    const entityRegistry = useEntityRegistry();

    const proposedName = actionRequest.params?.createGlossaryTermProposal?.glossaryTerm.name || '';
    const parentNode = actionRequest.params?.createGlossaryTermProposal?.glossaryTerm.parentNode || null;
    const entityName = entityRegistry.getEntityName(EntityType.GlossaryTerm);
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
