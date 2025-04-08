import React, { useState } from 'react';
import styled from 'styled-components';

import { StyledTag } from '@app/entityV2/shared/components/styled/StyledTag';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { TagProfileDrawer } from '@app/shared/tags/TagProfileDrawer';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, Tag } from '@types';

const Container = styled.span`
    display: block;
    max-width: fit-content;
`;

const Name = styled.div`
    overflow: hidden;
    text-overflow: ellipsis;
    display: block;
`;

const tagStyle = { cursor: 'pointer' };

interface Props {
    tag: Tag;
    fontSize?: number;
}

export default function TagLink({ tag, fontSize }: Props) {
    const entityRegistry = useEntityRegistry();

    const [tagProfileDrawerVisible, setTagProfileDrawerVisible] = useState(false);
    const [addTagUrn, setAddTagUrn] = useState('');

    const showTagProfileDrawer = (urn: string) => {
        setTagProfileDrawerVisible(true);
        setAddTagUrn(urn);
    };

    const closeTagProfileDrawer = () => {
        setTagProfileDrawerVisible(false);
    };

    const displayName = entityRegistry.getDisplayName(EntityType.Tag, tag);

    return (
        <>
            <HoverEntityTooltip entity={tag}>
                <Container data-testid={`tag-${displayName}`}>
                    <StyledTag
                        style={tagStyle}
                        onClick={() => showTagProfileDrawer(tag.urn)}
                        $colorHash={tag.urn}
                        $color={tag.properties?.colorHex}
                        closable={false}
                        fontSize={fontSize}
                    >
                        <Name>{displayName}</Name>
                    </StyledTag>
                </Container>
            </HoverEntityTooltip>
            {tagProfileDrawerVisible && (
                <TagProfileDrawer
                    closeTagProfileDrawer={closeTagProfileDrawer}
                    tagProfileDrawerVisible={tagProfileDrawerVisible}
                    urn={addTagUrn}
                />
            )}
        </>
    );
}
