import React from 'react';

import CreateGlossaryEntityContentView from '@app/actionrequestV2/item/CreateGlossaryEntityContentView';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { ActionRequest, EntityType } from '@src/types.generated';

interface Props {
    actionRequest: ActionRequest;
}

const CreateTermRequestItem = ({ actionRequest }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const proposedName = actionRequest.params?.createGlossaryTermProposal?.glossaryTerm?.name || '';
    const parentNode = actionRequest.params?.createGlossaryTermProposal?.glossaryTerm?.parentNode || null;
    const description = actionRequest.params?.createGlossaryTermProposal?.glossaryTerm?.description || '';
    const entityName = entityRegistry.getEntityName(EntityType.GlossaryTerm);
    return (
        <CreateGlossaryEntityContentView
            proposedName={proposedName}
            actionRequest={actionRequest}
            entityName={entityName || ''}
            parentNode={parentNode}
            description={description}
        />
    );
};

export default CreateTermRequestItem;
