import { Icon } from '@components';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React, { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { DocumentSourceLogo } from '@app/document/DocumentSourceLogo';
import { isDocumentUnpublished, isExternalDocument, pickTreeIcon } from '@app/document/utils/documentUtils';
import ResourceDocumentPillPopover from '@app/entityV2/shared/tabs/Documentation/components/ResourceDocumentPillPopover';
import { Pill, Popover } from '@src/alchemy-components';
import { PillRightIcon } from '@src/alchemy-components/components/Pills/types';

import { useGetDocumentLazyQuery } from '@graphql/document.generated';
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
    const [isHoverRequested, setIsHoverRequested] = useState(false);
    const [loadDocument, { called, data, loading }] = useGetDocumentLazyQuery();
    const hoverDocument = (data?.document as Document | null | undefined) ?? document;
    const handleOpenChange = useCallback(
        (isOpen: boolean) => {
            setIsHoverRequested(isOpen);
            if (isOpen && !called) {
                loadDocument({ variables: { urn: document.urn }, fetchPolicy: 'cache-first' });
            }
        },
        [called, document.urn, loadDocument],
    );

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
            content={<ResourceDocumentPillPopover document={hoverDocument} fallbackTitle={title} />}
            open={isHoverRequested && called && !loading}
            onOpenChange={handleOpenChange}
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
