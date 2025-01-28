import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import StripMarkdownText from '../../shared/components/styled/StripMarkdownText';
import { SidebarHeader } from '../../shared/containers/profile/sidebar/SidebarHeader';
import { useEntityData, useRouteToTab } from '../../../entity/shared/EntityContext';

const DescriptionTypography = styled(Typography.Paragraph)`
    max-width: 65ch;
`;

export default function GlossarySidebarAboutSection() {
    const { entityData }: any = useEntityData();
    const description = entityData?.glossaryTermInfo?.definition;
    const source = entityData?.glossaryTermInfo?.sourceRef;
    const sourceUrl = entityData?.glossaryTermInfo?.sourceUrl;
    const routeToTab = useRouteToTab();

    return (
        <div>
            <SidebarHeader title="About" />
            {description && (
                <DescriptionTypography>
                    <StripMarkdownText
                        limit={205}
                        readMore={
                            <Typography.Link onClick={() => routeToTab({ tabName: 'Documentation' })}>
                                Read More
                            </Typography.Link>
                        }
                    >
                        {description}
                    </StripMarkdownText>
                </DescriptionTypography>
            )}

            <SidebarHeader title="Source" />
            {source && (
                <DescriptionTypography>
                    {sourceUrl ? (
                        <a href={sourceUrl} target="_blank" rel="noreferrer">
                            {source}
                        </a>
                    ) : (
                        {
                            source,
                        }
                    )}
                </DescriptionTypography>
            )}
        </div>
    );
}
