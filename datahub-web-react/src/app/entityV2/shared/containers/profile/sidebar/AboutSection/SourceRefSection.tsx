import { LinkOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { StyledLink } from '@app/entityV2/shared/containers/profile/sidebar/LinkButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';

export default function SourceRefSection() {
    const { entityData } = useEntityData();

    const sourceUrl = entityData?.properties?.sourceUrl;
    const sourceRef = entityData?.properties?.sourceRef;

    if (!sourceRef) return null;

    return (
        <>
            <SidebarSection
                title="Source"
                content={
                    <>
                        <Typography.Paragraph>
                            {sourceUrl ? (
                                <StyledLink type="link" href={sourceUrl} target="_blank" rel="noreferrer">
                                    <LinkOutlined />
                                    {sourceRef}
                                </StyledLink>
                            ) : (
                                <span>{sourceRef}</span>
                            )}
                        </Typography.Paragraph>
                    </>
                }
            />
        </>
    );
}
