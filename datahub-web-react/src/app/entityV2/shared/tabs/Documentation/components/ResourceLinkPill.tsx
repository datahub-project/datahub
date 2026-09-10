import { Tooltip } from '@components';
import { PencilSimpleLine } from '@phosphor-icons/react/dist/csr/PencilSimpleLine';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { LinkIcon } from '@app/entityV2/shared/components/links/LinkIcon';
import { formatDateString } from '@app/entityV2/shared/containers/profile/utils';
import { ResourcePillMeta } from '@app/entityV2/shared/tabs/Documentation/components/ResourcePillMeta';
import { useLinkPermission } from '@app/entityV2/summary/links/useLinkPermission';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import { safeUrl } from '@app/shared/urlUtils';
import { Pill, Popover } from '@src/alchemy-components';
import { PillRightIcon } from '@src/alchemy-components/components/Pills/types';

import { InstitutionalMemoryMetadata } from '@types';

// The anchor makes the whole pill open the link in a new tab; the edit/remove
// buttons stopPropagation + preventDefault so they don't navigate.
const PillAnchor = styled.a`
    display: inline-flex;
    text-decoration: none;
    color: inherit;
`;

interface Props {
    link: InstitutionalMemoryMetadata;
    /**
     * Called when the pencil affordance is clicked (owner decides edit UX). The pencil
     * only renders when this is provided and the user has link permissions — omit it
     * for read-only surfaces like the entity sidebar.
     */
    onEdit?: (link: InstitutionalMemoryMetadata) => void;
    /**
     * Called when the X affordance is clicked (owner decides confirm UX). The X only
     * renders when this is provided and the user has link permissions.
     */
    onDelete?: (link: InstitutionalMemoryMetadata) => void;
}

/**
 * A link resource rendered as a gray pill with a hover popover showing who added it
 * and when. Edit/remove affordances are opt-in: each icon renders only when its
 * handler is supplied and the user has link permissions, so read-only surfaces (e.g.
 * the entity sidebar) get clean action-free pills. Shared by the entity summary, the
 * Documentation tab, the summary About section, and the sidebar.
 */
export function ResourceLinkPill({ link, onEdit, onDelete }: Props) {
    const { t } = useTranslation('entity.profile.summary');
    const { t: ta } = useTranslation('common.actions');
    const hasLinkPermissions = useLinkPermission();

    const label = link.description || link.label;
    const relativeTime = toRelativeTimeString(link.created?.time) || t('links.recently');

    const rightIcons: PillRightIcon[] = [];
    if (hasLinkPermissions && onEdit) {
        rightIcons.push({
            icon: PencilSimpleLine,
            ariaLabel: ta('edit'),
            testId: 'edit-link-button',
            onClick: (e) => {
                e.preventDefault();
                e.stopPropagation();
                onEdit(link);
            },
        });
    }
    if (hasLinkPermissions && onDelete) {
        rightIcons.push({
            icon: X,
            ariaLabel: ta('remove'),
            testId: 'remove-link-button',
            onClick: (e) => {
                e.preventDefault();
                e.stopPropagation();
                onDelete(link);
            },
        });
    }

    return (
        <Popover
            placement="top"
            content={
                <ResourcePillMeta
                    content={
                        <Trans
                            t={t}
                            i18nKey={link.actor ? 'links.addedBy' : 'links.added'}
                            values={{ relativeTime }}
                            components={{
                                time: link.created?.time ? (
                                    <Tooltip title={formatDateString(link.created.time)}>
                                        <span />
                                    </Tooltip>
                                ) : (
                                    <span />
                                ),
                            }}
                        />
                    }
                    actor={link.actor}
                />
            }
            mouseEnterDelay={0.3}
        >
            <PillAnchor href={safeUrl(link.url)} target="_blank" rel="noopener noreferrer">
                <Pill
                    label={label}
                    color="gray"
                    variant="filled"
                    clickable
                    customIconRenderer={() => <LinkIcon url={link.url} usePrimaryColor={false} />}
                    rightIcons={rightIcons}
                    dataTestId={`${link.url}-${label}`}
                />
            </PillAnchor>
        </Popover>
    );
}
