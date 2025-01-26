import React from 'react';
import { Link } from 'react-router-dom';
import { BookmarkSimple } from '@phosphor-icons/react';
import { ActionRequest, EntityType } from '../../../types.generated';
import { StyledTag } from '../../entity/shared/components/styled/StyledTag';
import { useEntityRegistry } from '../../useEntityRegistry';
import AddContentView from './AddContentView';
import MetadataAssociationRequestItem from './MetadataAssociationRequestItem';

type Props = {
    actionRequest: ActionRequest;
    showActionsButtons: boolean;
    onUpdate: () => void;
};

const REQUEST_TYPE_DISPLAY_NAME = 'Glossary Term Proposal';

/**
 * A list item representing a glossary term proposal request.
 */
export default function TermAssociationRequestItem({ actionRequest, showActionsButtons, onUpdate }: Props) {
    const entityRegistry = useEntityRegistry();

    const term = actionRequest.params?.glossaryTermProposal?.glossaryTerm;
    const termName = entityRegistry.getDisplayName(EntityType.GlossaryTerm, term);
    const termView = term && (
        <Link to={`/${entityRegistry.getPathName(EntityType.GlossaryTerm)}/${term.urn}`}>
            <StyledTag noMargin $color={null} style={{ marginRight: 2, marginLeft: 2 }}>
                {termName}
                <BookmarkSimple style={{ marginLeft: '2%' }} />
            </StyledTag>
        </Link>
    );

    const contentView = <AddContentView requestMetadataViews={[{ primary: termView }]} actionRequest={actionRequest} />;

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
