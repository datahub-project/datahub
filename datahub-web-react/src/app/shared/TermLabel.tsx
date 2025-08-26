import { BookmarkSimple, BookmarksSimple } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';

import { EntityType } from '@src/types.generated';

type Props = {
    name: string;
    type?: EntityType;
};

const TermName = styled.span`
    margin-left: 5px;
`;

export default function TermLabel({ name, type }: Props) {
    return (
        <div>
            {!type || (type === EntityType.GlossaryTerm && <BookmarkSimple />)}
            {type === EntityType.GlossaryNode && <BookmarksSimple size={16} />}
            <TermName>{name}</TermName>
        </div>
    );
}
