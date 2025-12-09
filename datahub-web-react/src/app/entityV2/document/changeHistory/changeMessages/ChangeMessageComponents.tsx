import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useParentDocumentTitle } from '@app/entityV2/document/changeHistory/hooks/useParentDocumentTitle';
import { isSystemActor } from '@app/entityV2/document/changeHistory/utils/changeUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Icon } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';

import { DocumentChangeType, EntityType } from '@types';

const ActionText = styled.div`
    font-size: 14px;
    line-height: 20px;
    display: flex;
    align-items: center;
    gap: 4px;
    color: ${colors.gray[600]};
`;

const ActorName = styled.span`
    font-weight: bold;
    display: inline-flex;
    align-items: center;
    gap: 4px;
`;

const ClickableText = styled(Link)`
    font-weight: bold;
    color: ${colors.gray[600]};
    text-decoration: none;
    cursor: pointer;

    &:hover {
        text-decoration: underline;
        color: ${colors.gray[800]};
    }
`;

const SeeVersionLink = styled.a`
    color: ${colors.primary[500]};
    cursor: pointer;
    text-decoration: none;

    &:hover {
        text-decoration: underline;
    }
`;

// ============================================================================
// Actor Display Component with Optional Sparkle Icon
// ============================================================================

interface ActorDisplayProps {
    actorName: string;
    actor: any;
}

/**
 * Renders an actor's name with a Sparkle icon if it's the DataHub AI system actor.
 * Makes the name clickable (except for system actors).
 */
const ActorDisplay: React.FC<ActorDisplayProps> = ({ actorName, actor }) => {
    const entityRegistry = useEntityRegistry();
    const isSystem = isSystemActor(actor);

    // System actors shouldn't be clickable
    if (isSystem) {
        return (
            <ActorName>
                <Icon icon="Sparkle" color="violet" size="sm" />
                {actorName}
            </ActorName>
        );
    }

    // Regular actors should be clickable links to their profile
    if (actor) {
        const actorUrl = entityRegistry.getEntityUrl(actor.type, actor.urn);
        return <ClickableText to={actorUrl}>{actorName}</ClickableText>;
    }

    // Fallback for no actor
    return <ActorName>{actorName}</ActorName>;
};

// ============================================================================
// Individual Change Message Components
// ============================================================================

interface ActorOnlyProps {
    actorName: string;
    actor: any;
}

interface ActorWithDetailsProps {
    actorName: string;
    actor: any;
    details: Record<string, string>;
}

export const CreatedMessage: React.FC<ActorOnlyProps> = ({ actorName, actor }) => (
    <ActionText>
        <ActorDisplay actorName={actorName} actor={actor} /> created document
    </ActionText>
);

export const TitleChangedMessage: React.FC<ActorWithDetailsProps> = ({ actorName, actor, details }) => (
    <ActionText>
        <ActorDisplay actorName={actorName} actor={actor} /> changed title to{' '}
        <ActorName>{details.newTitle || 'Untitled'}</ActorName>
    </ActionText>
);

interface TextChangedMessageProps {
    actorName: string;
    actor: any;
    onSeeVersion: () => void;
}

export const TextChangedMessage: React.FC<TextChangedMessageProps> = ({ actorName, actor, onSeeVersion }) => (
    <ActionText>
        <ActorDisplay actorName={actorName} actor={actor} /> edited the document.{' '}
        <SeeVersionLink onClick={onSeeVersion}>View previous</SeeVersionLink>
    </ActionText>
);

export const StateChangedMessage: React.FC<ActorWithDetailsProps> = ({ actorName, actor, details }) => {
    const { newState } = details;

    if (newState === 'PUBLISHED') {
        return (
            <ActionText>
                <ActorDisplay actorName={actorName} actor={actor} /> published document
            </ActionText>
        );
    }

    if (newState === 'UNPUBLISHED') {
        return (
            <ActionText>
                <ActorDisplay actorName={actorName} actor={actor} /> unpublished document
            </ActionText>
        );
    }

    return (
        <ActionText>
            <ActorDisplay actorName={actorName} actor={actor} /> changed state to {newState}
        </ActionText>
    );
};

export const ParentChangedMessage: React.FC<ActorWithDetailsProps> = ({ actorName, actor, details }) => {
    const entityRegistry = useEntityRegistry();
    const { oldParent, newParent } = details;

    // Fetch titles for both old and new parents if they exist
    const { title: oldParentTitle } = useParentDocumentTitle(oldParent);
    const { title: newParentTitle } = useParentDocumentTitle(newParent);

    // Case 1: Moved from one parent to another
    if (oldParent && newParent) {
        const oldParentUrl = entityRegistry.getEntityUrl(EntityType.Document, oldParent);
        const newParentUrl = entityRegistry.getEntityUrl(EntityType.Document, newParent);
        return (
            <ActionText>
                <ActorDisplay actorName={actorName} actor={actor} /> moved document from{' '}
                <ClickableText to={oldParentUrl}>{oldParentTitle}</ClickableText> to{' '}
                <ClickableText to={newParentUrl}>{newParentTitle}</ClickableText>
            </ActionText>
        );
    }

    // Case 2: Moved from root to a parent (or just moved to a parent)
    if (newParent) {
        const newParentUrl = entityRegistry.getEntityUrl(EntityType.Document, newParent);
        return (
            <ActionText>
                <ActorDisplay actorName={actorName} actor={actor} /> moved document to{' '}
                <ClickableText to={newParentUrl}>{newParentTitle}</ClickableText>
            </ActionText>
        );
    }

    // Case 3: Moved to root level (or parent was removed)
    if (oldParent) {
        const oldParentUrl = entityRegistry.getEntityUrl(EntityType.Document, oldParent);
        return (
            <ActionText>
                <ActorDisplay actorName={actorName} actor={actor} /> moved document from{' '}
                <ClickableText to={oldParentUrl}>{oldParentTitle}</ClickableText> to root level
            </ActionText>
        );
    }

    // Fallback: Moved to root level (no old parent specified)
    return (
        <ActionText>
            <ActorDisplay actorName={actorName} actor={actor} /> moved document to root level
        </ActionText>
    );
};

export const DeletedMessage: React.FC<ActorOnlyProps> = ({ actorName, actor }) => (
    <ActionText>
        <ActorDisplay actorName={actorName} actor={actor} /> deleted document
    </ActionText>
);

interface DefaultMessageProps {
    actorName: string;
    actor: any;
    description: string;
}

export const DefaultMessage: React.FC<DefaultMessageProps> = ({ actorName, actor, description }) => (
    <ActionText>
        <ActorDisplay actorName={actorName} actor={actor} /> {description}
    </ActionText>
);

// ============================================================================
// Main Router Component
// ============================================================================

interface ChangeMessageProps {
    changeType: DocumentChangeType;
    actorName: string;
    actor: any;
    details: Record<string, string>;
    description: string;
    onSeeVersion: () => void;
}

/**
 * Routes to the appropriate change message component based on the change type.
 *
 * To add a new change type:
 * 1. Create a new component above (e.g., `MyNewChangeMessage`)
 * 2. Add a case for it in the switch statement below
 * 3. Pass the necessary props to your component
 */
export const ChangeMessage: React.FC<ChangeMessageProps> = ({
    changeType,
    actorName,
    actor,
    details,
    description,
    onSeeVersion,
}) => {
    switch (changeType) {
        case DocumentChangeType.Created:
            return <CreatedMessage actorName={actorName} actor={actor} />;

        case DocumentChangeType.TitleChanged:
            return <TitleChangedMessage actorName={actorName} actor={actor} details={details} />;

        case DocumentChangeType.TextChanged:
            return <TextChangedMessage actorName={actorName} actor={actor} onSeeVersion={onSeeVersion} />;

        case DocumentChangeType.StateChanged:
            return <StateChangedMessage actorName={actorName} actor={actor} details={details} />;

        case DocumentChangeType.ParentChanged:
            return <ParentChangedMessage actorName={actorName} actor={actor} details={details} />;

        case DocumentChangeType.Deleted:
            return <DeletedMessage actorName={actorName} actor={actor} />;

        default:
            return <DefaultMessage actorName={actorName} actor={actor} description={description} />;
    }
};
