import React from 'react';
import { Tag, Typography } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { ANTD_GRAY } from '../../shared/constants';
import { IconStyleType } from '../../Entity';
import NoMarkdownViewer from '../../shared/components/styled/StripMarkdownText';
import SearchTextHighlighter from '../../../searchV2/matches/SearchTextHighlighter';

const PreviewContainer = styled.div`
    margin-bottom: 4px;
    display: flex;
    width: 100%;
    justify-content: space-between;
    align-items: center;
`;

const PlatformInfo = styled.div`
    margin-bottom: 8px;
    display: flex;
    align-items: center;
    height: 24px;
`;

const TitleContainer = styled.div`
    margin-bottom: 8px;
`;

const PreviewImage = styled.div`
    max-height: 18px;
    width: auto;
    object-fit: contain;
    margin-right: 10px;
    background-color: transparent;
`;

const EntityTitle = styled(Typography.Text)`
    &&& {
        margin-bottom: 0;
        font-size: 16px;
        font-weight: 600;
        vertical-align: middle;
    }
`;

const PlatformText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 700;
    color: ${ANTD_GRAY[7]};
`;

const DescriptionContainer = styled.div`
    margin-top: 5px;
`;

const MemberCountContainer = styled.span`
    margin-left: 12px;
    margin-right: 12px;
`;

export const Preview = ({
    urn,
    name,
    description,
    membersCount,
}: {
    urn: string;
    name: string;
    description?: string | null;
    membersCount?: number;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const url = entityRegistry.getEntityUrl(EntityType.CorpGroup, urn);

    return (
        <PreviewContainer>
            <div>
                <Link to={url}>
                    <TitleContainer>
                        <PlatformInfo>
                            <PreviewImage>
                                {entityRegistry.getIcon(EntityType.CorpGroup, 20, IconStyleType.HIGHLIGHT)}
                            </PreviewImage>
                            <PlatformText>{entityRegistry.getEntityName(EntityType.CorpGroup)}</PlatformText>
                        </PlatformInfo>
                        <Link to={url}>
                            <EntityTitle>{name ? <SearchTextHighlighter field="name" text={name} /> : urn}</EntityTitle>
                            <MemberCountContainer>
                                <Tag>{membersCount} members</Tag>
                            </MemberCountContainer>
                        </Link>
                    </TitleContainer>
                </Link>
                {description && description.length > 0 && (
                    <DescriptionContainer>
                        <NoMarkdownViewer
                            limit={200}
                            customRender={(text) => <SearchTextHighlighter field="description" text={text} />}
                        >
                            {description}
                        </NoMarkdownViewer>
                    </DescriptionContainer>
                )}
            </div>
        </PreviewContainer>
    );
};
