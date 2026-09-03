import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import CompactMarkdownViewer from '@app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';
import { getExternalUrlDisplayName } from '@app/entityV2/shared/utils';
import { Alert } from '@src/alchemy-components';

import { Document } from '@types';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

// Strip the document title if the source system echoes it as the first line of content.
// Handles plain text, markdown headings (# Title), and markdown links ([Title](url)).
function stripLeadingTitle(content: string, title: string): string {
    if (!title) return content;
    const lines = content.split('\n');
    const firstLine = lines[0].trim();
    const normalized = firstLine
        .replace(/^#+\s*/, '')
        .replace(/^\[(.+?)\]\(.+?\)$/, '$1')
        .trim();
    if (normalized.toLowerCase() === title.toLowerCase()) {
        return lines.slice(1).join('\n').trimStart();
    }
    return content;
}

export const ExternalDocumentInlineContentSection: React.FC = () => {
    const { t } = useTranslation('entity.types');
    const { entityData } = useEntityData();
    const document = entityData as Document;

    const rawContent = document?.info?.contents?.text?.trim() || '';
    if (!rawContent) {
        return null;
    }

    const title = document?.info?.title || '';
    const content = stripLeadingTitle(rawContent, title);
    const platform = getExternalUrlDisplayName(entityData) || t('document.sourceSystemFallback');

    return (
        <Section data-testid="external-document-inline-content-section">
            <Alert
                variant="info"
                title={t('document.textExtractedFrom', { platform })}
                data-testid="external-document-inline-banner"
            />
            <CompactMarkdownViewer content={content} lineLimit={null} hideShowMore scrollableY={false} />
        </Section>
    );
};
