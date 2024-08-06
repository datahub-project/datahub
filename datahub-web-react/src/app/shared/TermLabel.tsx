import React from 'react';
import { BookOutlined } from '@ant-design/icons';
import styled from 'styled-components';

import { EntityType } from '@src/types.generated';
import GlossaryIcon from '@images/glossary_collections_bookmark.svg';

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
            {!type || (type === EntityType.GlossaryTerm && <BookOutlined />)}
            {type === EntityType.GlossaryNode && (
                <img src={GlossaryIcon} alt="Glossary Node" style={{ width: '16px', height: '16px' }} />
            )}
            <TermName>{name}</TermName>
        </div>
    );
}
