import { LinkOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React from 'react';
import { useEntityData } from '../../../../EntityContext';
import { SidebarHeader } from '../SidebarHeader';
import { StyledLink } from '../LinkButton';

export default function SourceRefSection() {
    const { entityData } = useEntityData();

    const sourceUrl = entityData?.properties?.sourceUrl;
    const sourceRef = entityData?.properties?.sourceRef;

    if (!sourceRef) return null;

    return (
        <>
            <SidebarHeader title="Source" />
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
    );
}
