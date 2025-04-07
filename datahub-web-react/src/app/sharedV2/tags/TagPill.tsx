import React from 'react';
import { StyledTag } from '@src/app/entityV2/shared/components/styled/StyledTag';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { EntityType, Tag } from '@src/types.generated';
import styled from 'styled-components';

interface TagPillProps {
    tag: Tag;
    fontSize?: number;
    style?: React.CSSProperties;
    onClick?: React.MouseEventHandler<HTMLSpanElement>;
}

const Name = styled.div`
    overflow: hidden;
    text-overflow: ellipsis;
    display: block;
`;

const Container = styled.span`
    display: block;
    max-width: fit-content;
`;

export default function TagPill({ tag, style, fontSize, onClick }: TagPillProps) {
    const entityRegistry = useEntityRegistryV2();
    const displayName = entityRegistry.getDisplayName(EntityType.Tag, tag);

    return (
        <Container data-testid={`tag-${displayName}`}>
            <StyledTag
                style={style}
                onClick={onClick}
                $colorHash={tag.urn}
                $color={tag.properties?.colorHex}
                closable={false}
                fontSize={fontSize}
            >
                <Name>{displayName}</Name>
            </StyledTag>
        </Container>
    );
}
