import { StyledTag } from '@src/app/entity/shared/components/styled/StyledTag';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { ActionRequest, ActionRequestResult, EntityType } from '@src/types.generated';
import { BookmarkSimple } from 'phosphor-react';
import React from 'react';
import { Link } from 'react-router-dom';
import AddContentView from './AddContentView';

interface Props {
    actionRequest: ActionRequest;
}

const TermAssociationRequestItem = ({ actionRequest }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const terms = actionRequest.params?.glossaryTermProposal?.glossaryTerm
        ? [actionRequest.params?.glossaryTermProposal?.glossaryTerm]
        : actionRequest.params?.glossaryTermProposal?.glossaryTerms;

    const termView = terms && (
        <>
            {terms.map((term) => {
                const termName = term && entityRegistry.getDisplayName(EntityType.GlossaryTerm, term);

                return (
                    <Link to={`/${entityRegistry.getPathName(EntityType.GlossaryTerm)}/${term.urn}`}>
                        <StyledTag
                            noMargin
                            $color={null}
                            style={{ marginRight: 2, marginLeft: 2 }}
                            $isApproved={actionRequest.result === ActionRequestResult.Accepted}
                        >
                            {termName}
                            <BookmarkSimple style={{ marginLeft: '2%' }} />
                        </StyledTag>
                    </Link>
                );
            })}
        </>
    );
    return <AddContentView requestMetadataViews={[{ primary: termView }]} actionRequest={actionRequest} />;
};

export default TermAssociationRequestItem;
