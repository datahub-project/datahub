import { Icon, Tooltip } from '@components';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';

import { DocumentSourceLogo } from '@app/document/DocumentSourceLogo';
import { isDocumentUnpublished, isExternalDocument, pickTreeIcon } from '@app/document/utils/documentUtils';
import { formatDateString } from '@app/entityV2/shared/containers/profile/utils';
import { ResourcePillMeta } from '@app/entityV2/shared/tabs/Documentation/components/ResourcePillMeta';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import { Pill, Popover } from '@src/alchemy-components';
import { PillRightIcon } from '@src/alchemy-components/components/Pills/types';

import { Document } from '@types';

interface Props {
    document: Document;
    /** Opens the document (owner decides whether that's a modal, route, etc.). */
    onClick: (documentUrn: string) => void;
    /** When provided AND `canRemove` is true, renders a trailing X that unlinks the doc. */
    onRemove?: (documentUrn: string) => void;
    canRemove?: boolean;
}

/**
 * A document resource rendered as a gray pill with a hover popover showing who last
 * edited it and when. External (ingested) documents show their platform logo; native
 * ones show a file glyph. Shared by the entity summary and Documentation tab Resources
 * sections.
 */
export function ResourceDocumentPill({ document, onClick, onRemove, canRemove = false }: Props) {
    const { t } = useTranslation('entity.profile.summary');
    const { t: ta } = useTranslation('common.actions');

    const title = document.info?.title || t('links.untitledDocument');
    const lastModified = document.info?.lastModified;
    const actor = lastModified?.actor;
    const relativeTime = toRelativeTimeString(lastModified?.time) || t('links.recently');

    const DocumentGlyph = pickTreeIcon({ hasChildren: false, isUnpublished: isDocumentUnpublished(document) });
    const isExternal = isExternalDocument(document) && document.platform;

    const rightIcons: PillRightIcon[] =
        canRemove && onRemove
            ? [
                  {
                      icon: X,
                      ariaLabel: ta('remove'),
                      testId: 'remove-related-document-button',
                      onClick: (e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          onRemove(document.urn);
                      },
                  },
              ]
            : [];

    return (
        <Popover
            placement="top"
            content={
                <ResourcePillMeta
                    content={
                        <Trans
                            t={t}
                            i18nKey={actor ? 'links.editedBy' : 'links.edited'}
                            values={{ relativeTime }}
                            components={{
                                time: lastModified?.time ? (
                                    <Tooltip title={formatDateString(lastModified.time)}>
                                        <span />
                                    </Tooltip>
                                ) : (
                                    <span />
                                ),
                            }}
                        />
                    }
                    actor={actor}
                />
            }
            mouseEnterDelay={0.3}
        >
            <Pill
                label={title}
                color="gray"
                variant="filled"
                clickable
                customIconRenderer={() =>
                    isExternal ? (
                        <DocumentSourceLogo
                            platform={document.platform}
                            size={14}
                            fallback={<Icon icon={DocumentGlyph} size="md" />}
                        />
                    ) : (
                        <Icon icon={DocumentGlyph} size="md" />
                    )
                }
                rightIcons={rightIcons}
                onPillClick={() => onClick(document.urn)}
                dataTestId={`related-context-document-${document.urn.split(':').pop()}`}
            />
        </Popover>
    );
}
