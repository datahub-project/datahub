/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
