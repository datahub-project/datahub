import { StyledTag } from '@src/app/entity/shared/components/styled/StyledTag';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { ActionRequest, EntityType } from '@src/types.generated';
import { BookmarkSimple } from 'phosphor-react';
import React from 'react';
import { Link } from 'react-router-dom';
import AddContentView from './AddContentView';

interface Props {
    actionRequest: ActionRequest;
}

const TermAssociationRequestItem = ({ actionRequest }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const term =
        actionRequest.params?.glossaryTermProposal?.glossaryTerm ||
        actionRequest.params?.glossaryTermProposal?.glossaryTerms?.[0];
    const termName = term && entityRegistry.getDisplayName(EntityType.GlossaryTerm, term);
    const termView = term && (
        <Link to={`/${entityRegistry.getPathName(EntityType.GlossaryTerm)}/${term.urn}`}>
            <StyledTag noMargin $color={null} style={{ marginRight: 2, marginLeft: 2 }}>
                {termName}
                <BookmarkSimple style={{ marginLeft: '2%' }} />
            </StyledTag>
        </Link>
    );
    return <AddContentView requestMetadataViews={[{ primary: termView }]} actionRequest={actionRequest} />;
};

export default TermAssociationRequestItem;
