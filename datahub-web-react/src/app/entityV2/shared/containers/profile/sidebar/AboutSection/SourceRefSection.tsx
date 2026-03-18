import { Button, Icon } from '@components';
import { Link } from '@phosphor-icons/react/dist/csr/Link';
import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';

export default function SourceRefSection() {
    const { entityData } = useEntityData();

    const sourceUrl = entityData?.properties?.sourceUrl;
    const sourceRef = entityData?.properties?.sourceRef;

    if (!sourceRef) return null;

    return (
        <SidebarSection
            title="Source"
            content={
                sourceUrl ? (
                    <a href={sourceUrl} target="_blank" rel="noreferrer" style={{ textDecoration: 'none' }}>
                        <Button variant="text" color="violet">
                            <Icon icon={Link} size="md" color="inherit" />
                            {sourceRef}
                        </Button>
                    </a>
                ) : (
                    <span>{sourceRef}</span>
                )
            }
        />
    );
}
