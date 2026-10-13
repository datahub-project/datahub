import { Pill, Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import AvatarPillWithLinkAndHover from '@components/components/Avatar/AvatarPillWithLinkAndHover';

import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { CorpGroup, CorpUser } from '@types';

// Rich content shown when a user hovers a Resource pill (link or document).
// The caller is responsible for rendering the "Edited/Added N ago" text via `<Trans>`
// (kept at the callsite so the lint rule that whitelists react-i18next's `i18nKey`
// prop keeps working without eslint-disable comments).

const PopoverRoot = styled.div`
    display: flex;
    flex-direction: column;
    gap: 6px;
    max-width: 280px;
`;

interface Props {
    /** Rendered text node — typically a `<Trans>` for "Edited N ago" / "Added N ago". */
    content: React.ReactNode;
    actor?: CorpUser | CorpGroup | null;
    /** Optional free-form resource type, shown as a small badge. */
    resourceType?: string | null;
    /** Optional longer description of the resource. */
    resourceDescription?: string | null;
}

/**
 * Rich content for the hover popover on a Resource pill. Intended to be passed as the
 * `content` prop of a `<Popover>`.
 */
export function ResourcePillMeta({ content, actor, resourceType, resourceDescription }: Props) {
    const entityRegistry = useEntityRegistryV2();

    return (
        <PopoverRoot>
            {resourceType && (
                <div>
                    <Pill label={resourceType} size="sm" color="gray" variant="outline" dataTestId="link-type-badge" />
                </div>
            )}
            {resourceDescription && <Text size="sm">{resourceDescription}</Text>}
            <Text size="sm" color="textSecondary">
                {content}
            </Text>
            {actor && <AvatarPillWithLinkAndHover user={actor} size="sm" entityRegistry={entityRegistry} />}
        </PopoverRoot>
    );
}
