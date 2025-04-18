import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { ActionRequest, ActionRequestResult, EntityType } from '@src/types.generated';
import React from 'react';
import { Link } from 'react-router-dom';
import ProposedTermPill from '@src/app/sharedV2/tags/term/ProposedTermPill';
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
                return (
                    <Link to={`/${entityRegistry.getPathName(EntityType.GlossaryTerm)}/${term.urn}`}>
                        <ProposedTermPill
                            term={term}
                            isApproved={actionRequest.result === ActionRequestResult.Accepted}
                            showClockIcon={false}
                        />
                    </Link>
                );
            })}
        </>
    );
    return <AddContentView requestMetadataViews={[{ primary: termView }]} actionRequest={actionRequest} />;
};

export default TermAssociationRequestItem;
