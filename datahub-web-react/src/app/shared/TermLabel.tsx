import { BookOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

type Props = {
    name: string;
};

const TermName = styled.span`
    margin-left: 5px;
`;

export default function TermLabel({ name }: Props) {
    return (
        <div>
            <BookOutlined />
            <TermName>{name}</TermName>
        </div>
    );
}
