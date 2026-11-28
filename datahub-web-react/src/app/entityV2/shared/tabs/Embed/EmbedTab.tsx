import { EmptyState } from '@components';
import { MonitorPlay } from '@phosphor-icons/react/dist/csr/MonitorPlay';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { safeUrl } from '@utils/urlUtils';

const EmbedContainer = styled.div`
    width: 100%;
    height: 100%;
`;

const StyledIframe = styled.iframe`
    width: 100%;
    height: 100%;
`;

const EmptyContainer = styled.div`
    padding-top: 60px;
`;

export const EmbedTab = () => {
    const { entityData } = useEntityData();
    const { t } = useTranslation('entity.profile.tabs');
    const embedRenderUrl = entityData?.embed?.renderUrl;
    return (
        <EmbedContainer>
            {(embedRenderUrl && (
                <StyledIframe src={safeUrl(embedRenderUrl)} title={entityData?.urn} frameBorder={0} />
            )) || (
                <EmptyContainer>
                    <EmptyState icon={MonitorPlay} title={t('embed.noPreview')} />
                </EmptyContainer>
            )}
        </EmbedContainer>
    );
};
