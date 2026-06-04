import React from 'react';
import { Link } from 'react-router-dom';

import { getActorDisplayName, isSystemActor } from '@app/entityV2/document/changeHistory/utils/changeUtils';
import { Avatar } from '@src/alchemy-components';
import { HoverEntityTooltip } from '@src/app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

import { DocumentChange } from '@types';

interface DocumentChangeTimelineDotProps {
    change: DocumentChange;
}

export const DocumentChangeTimelineDot: React.FC<DocumentChangeTimelineDotProps> = ({ change }) => {
    const entityRegistry = useEntityRegistryV2();
    const { actor } = change;

    if (!actor) {
        // If no actor, show a system icon or default avatar
        /* eslint-disable i18next/no-literal-string -- (untranslated-text) Avatar name fallback for missing actor; mirrors getActorDisplayName('System') in changeUtils.ts, kept in EN */
        return <Avatar name="System" size="xl" />;
        /* eslint-enable i18next/no-literal-string */
    }

    const avatarUrl = actor.editableProperties?.pictureLink || undefined;
    const actorName = getActorDisplayName(actor, entityRegistry);

    // For DataHub AI (system actor), use "AI" as the name to show "AI" initials
    /* eslint-disable i18next/no-literal-string -- (untranslated-text) avatar initials seed for the DataHub AI system actor, not a sentence */
    const avatarName = isSystemActor(actor) ? 'AI' : actorName;
    /* eslint-enable i18next/no-literal-string */

    return (
        <HoverEntityTooltip entity={actor} showArrow={false}>
            <Link to={`${entityRegistry.getEntityUrl(actor.type, actor.urn)}`}>
                <Avatar name={avatarName} imageUrl={avatarUrl} size="xl" />
            </Link>
        </HoverEntityTooltip>
    );
};
