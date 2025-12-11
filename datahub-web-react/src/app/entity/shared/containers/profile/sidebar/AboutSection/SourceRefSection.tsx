/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { LinkOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { StyledLink } from '@app/entity/shared/containers/profile/sidebar/LinkButton';
import { SidebarHeader } from '@app/entity/shared/containers/profile/sidebar/SidebarHeader';

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
