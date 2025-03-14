import React, { useState } from 'react';
import styled from 'styled-components';
import { EntityType, Tag } from '../../../types.generated';
import { StyledTag } from '../../entityV2/shared/components/styled/StyledTag';
import { HoverEntityTooltip } from '../../recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '../../useEntityRegistry';
import { TagProfileDrawer } from '../../shared/tags/TagProfileDrawer';

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
    enableTooltip?: boolean;
    enableDrawer?: boolean;
}

export default function TagLink({ tag, fontSize, enableTooltip = true, enableDrawer = true }: Props) {
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
            <HoverEntityTooltip entity={tag} canOpen={enableTooltip}>
                <Container data-testid={`tag-${displayName}`}>
                    <StyledTag
                        style={tagStyle}
                        onClick={() => showTagProfileDrawer(tag.urn)}
                        // TODO::? why urn in colorHash???
                        $colorHash={tag.urn}
                        $color={tag.properties?.colorHex}
                        closable={false}
                        fontSize={fontSize}
                    >
                        <Name>{displayName}</Name>
                    </StyledTag>
                </Container>
            </HoverEntityTooltip>
            {tagProfileDrawerVisible && enableDrawer && (
                <TagProfileDrawer
                    closeTagProfileDrawer={closeTagProfileDrawer}
                    tagProfileDrawerVisible={tagProfileDrawerVisible}
                    urn={addTagUrn}
                />
            )}
        </>
    );
}
