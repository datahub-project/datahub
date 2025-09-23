import { Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import SearchTextHighlighter from '@app/search/matches/SearchTextHighlighter';
import { CustomAvatar } from '@app/shared/avatar';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

const PreviewContainer = styled.div`
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

const AvatarContainer = styled.div`
    margin-right: 60px;
`;

export const Preview = ({
    urn,
    name,
    title,
    photoUrl,
}: {
    urn: string;
    name: string;
    photoUrl?: string | undefined;
    title?: string | undefined;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const url = entityRegistry.getEntityUrl(EntityType.CorpUser, urn);

    return (
        <PreviewContainer>
            <div>
                <Link to={url}>
                    <TitleContainer>
                        <PlatformInfo>
                            <PreviewImage>
                                {entityRegistry.getIcon(EntityType.CorpUser, 20, IconStyleType.HIGHLIGHT)}
                            </PreviewImage>
                            <PlatformText>{entityRegistry.getEntityName(EntityType.CorpUser)}</PlatformText>
                        </PlatformInfo>
                        <Link to={url}>
                            <EntityTitle>{name ? <SearchTextHighlighter field="name" text={name} /> : urn}</EntityTitle>
                        </Link>
                    </TitleContainer>
                </Link>
                {title && (
                    <TitleContainer>
                        <SearchTextHighlighter field="title" text={title} />
                    </TitleContainer>
                )}
            </div>
            <Link to={url}>
                <AvatarContainer>
                    <CustomAvatar size={48} photoUrl={photoUrl} name={name || undefined} />
                </AvatarContainer>
            </Link>
        </PreviewContainer>
    );
};
