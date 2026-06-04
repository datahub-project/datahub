import { Button, Icon } from '@components';
import { Link } from '@phosphor-icons/react/dist/csr/Link';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { safeUrl } from '@app/shared/urlUtils';

const StyledAnchor = styled.a`
    text-decoration: none;
`;

export default function SourceRefSection() {
    const { t } = useTranslation('entity.shared.containers');
    const { entityData } = useEntityData();

    const sourceUrl = entityData?.properties?.sourceUrl;
    const sourceRef = entityData?.properties?.sourceRef;

    if (!sourceRef) return null;

    return (
        <SidebarSection
            title={t('sidebar.about.sourceTitle')}
            content={
                sourceUrl ? (
                    <StyledAnchor href={safeUrl(sourceUrl)} target="_blank" rel="noreferrer">
                        <Button variant="text" color="violet">
                            <Icon icon={Link} size="md" color="inherit" />
                            {sourceRef}
                        </Button>
                    </StyledAnchor>
                ) : (
                    <span>{sourceRef}</span>
                )
            }
        />
    );
}
