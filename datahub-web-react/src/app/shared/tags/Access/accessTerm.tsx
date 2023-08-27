import { BookOutlined } from '@ant-design/icons';
import { Tag } from 'antd';
import React from 'react';
import styled from 'styled-components';
import Highlight from 'react-highlighter';

const highlightMatchStyle = { background: '#ffe58f', padding: '0' };

const StyledTag = styled(Tag)<{ fontSize?: number }>`
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
`;

interface Props {
    data?: string;
    highlightText?: string;
    fontSize?: number;
}

export default function AccessTermstyle({ data, fontSize, highlightText }: Props) {
    return (
        <StyledTag style={{ cursor: 'pointer' }} fontSize={fontSize}>
            <BookOutlined style={{ marginRight: '4px' }} />
            <Highlight style={{ marginLeft: 0 }} matchStyle={highlightMatchStyle} search={highlightText}>
                {data?.slice(0, 10)} {data && data.length > 10 ? '...' : ''}
            </Highlight>
        </StyledTag>
    );
}
