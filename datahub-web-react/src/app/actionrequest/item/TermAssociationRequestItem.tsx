import { BookOutlined } from '@ant-design/icons';
import React from 'react';
import { Link } from 'react-router-dom';
import { ActionRequest, EntityType } from '../../../types.generated';
import { StyledTag } from '../../entity/shared/components/styled/StyledTag';
import { useEntityRegistry } from '../../useEntityRegistry';
import AddContentView from './AddContentView';
import MetadataAssociationRequestItem from './MetadataAssociationRequestItem';

type Props = {
    actionRequest: ActionRequest;
    onUpdate: () => void;
    showActionsButtons: boolean;
};

const REQUEST_TYPE_DISPLAY_NAME = 'Glossary Term Proposal';

/**
 * A list item representing a glossary term proposal request.
 */
export default function TermAssociationRequestItem({ actionRequest, onUpdate, showActionsButtons }: Props) {
    const entityRegistry = useEntityRegistry();

    const term = actionRequest.params?.glossaryTermProposal?.glossaryTerm;
    const termName = entityRegistry.getDisplayName(EntityType.GlossaryTerm, term);
    const termView = term && (
        <Link to={`/${entityRegistry.getPathName(EntityType.GlossaryTerm)}/${term.urn}`}>
            <StyledTag $color={null} style={{ marginRight: 2, marginLeft: 2 }}>
                {termName}
                <BookOutlined style={{ marginLeft: '2%' }} />
            </StyledTag>
        </Link>
    );

    const contentView = (
        <AddContentView
            showActionsButtons={showActionsButtons}
            requestMetadataView={termView}
            actionRequest={actionRequest}
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
