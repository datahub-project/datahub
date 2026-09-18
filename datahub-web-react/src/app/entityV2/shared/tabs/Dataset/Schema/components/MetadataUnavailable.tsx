import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const Marker = styled.span`
    font-size: 12px;
    color: ${(props) => props.theme.colors.textDisabled};
`;

/**
 * Placeholder for a Phase 2 metadata cell (description/tags/terms/structured properties)
 * whose data could not be loaded. Distinguishes "failed to load" from "genuinely empty" so
 * a blank cell after a full-metadata error does not read as "no tags on this field".
 */
export default function MetadataUnavailable() {
    const { t } = useTranslation('entity.profile.schema');
    return <Marker data-testid="metadata-unavailable">{t('schemaTab.metadataUnavailable')}</Marker>;
}
