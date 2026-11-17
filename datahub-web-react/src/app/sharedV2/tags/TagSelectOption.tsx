import React from 'react';
import styled from 'styled-components';

import { isTag } from '@app/entityV2/tag/utils';
import TagLink from '@app/sharedV2/tags/TagLink';

import { Entity } from '@types';

const TagOptionContainer = styled.span`
    display: block;
    max-width: 100%;
`;

export function TagSelectOption({ entity }: { entity: Entity }) {
    const tag = (isTag(entity) && entity) || undefined;
    if (tag === undefined) return null;

    return (
        <TagOptionContainer>
            <TagLink tag={tag} enableTooltip={false} enableDrawer={false} />
        </TagOptionContainer>
    );
}
