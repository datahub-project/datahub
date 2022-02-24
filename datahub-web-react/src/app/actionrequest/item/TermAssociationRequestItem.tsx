import { BookOutlined } from '@ant-design/icons';
import React from 'react';
import { Link } from 'react-router-dom';
import { ActionRequest, EntityType } from '../../../types.generated';
import { StyledTag } from '../../entity/shared/components/styled/StyledTag';
import { useEntityRegistry } from '../../useEntityRegistry';
import MetadataAssociationRequestItem from './MetadataAssociationRequestItem';

type Props = {
    actionRequest: ActionRequest;
    onUpdate: () => void;
};

const REQUEST_TYPE_DISPLAY_NAME = 'Glossary Term Proposal';

/**
 * A list item representing a glossary term proposal request.
 */
export default function TermAssociationRequestItem({ actionRequest, onUpdate }: Props) {
    const entityRegistry = useEntityRegistry();

    const term = actionRequest.params?.glossaryTermProposal?.glossaryTerm;
    const termView = term && (
        <Link to={`/${entityRegistry.getPathName(EntityType.GlossaryTerm)}/${term.urn}`}>
            <StyledTag $color={null} style={{ marginRight: 2, marginLeft: 2 }}>
                {term?.name}
                <BookOutlined style={{ marginLeft: '2%' }} />
            </StyledTag>
        </Link>
    );

    return (
        <MetadataAssociationRequestItem
            requestTypeDisplayName={REQUEST_TYPE_DISPLAY_NAME}
            requestMetadataView={termView}
            actionRequest={actionRequest}
            onUpdate={onUpdate}
        />
    );
}
