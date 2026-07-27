import React, { useState } from 'react';
import styled from 'styled-components';

import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { TagProfileDrawer } from '@app/shared/tags/TagProfileDrawer';
import TagPill from '@app/sharedV2/tags/TagPill';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, Tag } from '@types';

const Container = styled.span`
    display: block;
    max-width: fit-content;
`;

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
                <Container data-testid={`tag-${displayName}`} onClick={() => showTagProfileDrawer(tag.urn)}>
                    <TagPill
                        name={displayName}
                        color={tag.properties?.colorHex}
                        colorHash={tag.urn}
                        size={fontSize && fontSize <= 10 ? 'sm' : 'md'}
                        clickable
                    />
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
