import React from 'react';

import CreateGlossaryEntityContentView from '@app/actionrequest/item/CreateGlossaryEntityContentView';
import MetadataAssociationRequestItem from '@app/actionrequest/item/MetadataAssociationRequestItem';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ActionRequest, EntityType } from '@types';

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
    const description = actionRequest.params?.createGlossaryTermProposal?.glossaryTerm.description || '';
    const entityName = entityRegistry.getEntityName(EntityType.GlossaryTerm);
    const contentView = (
        <CreateGlossaryEntityContentView
            proposedName={proposedName}
            actionRequest={actionRequest}
            entityName={entityName || ''}
            parentNode={parentNode}
            description={description}
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
