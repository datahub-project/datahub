import React from 'react';
import styled from 'styled-components';
import { ExpandOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import CopyQuery from './CopyQuery';

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: right;
`;

const Actions = styled.div<{ opacity?: number }>`
    padding: 0px;
    height: 0px;
    transform: translate(-12px, 12px);
    opacity: ${(props) => props.opacity || 1.0};
`;

const ExpandButton = styled(Button)`
    margin-left: 8px;
`;

export type Props = {
    query: string;
    focused: boolean;
    onClickExpand?: (newQuery) => void;
};

export default function QueryCardHeader({ query, focused, onClickExpand }: Props) {
    return (
        <Header>
            <Actions opacity={(!focused && 0.3) || 1.0}>
                <CopyQuery query={query} />
                <ExpandButton onClick={onClickExpand}>
                    <ExpandOutlined />
                </ExpandButton>
            </Actions>
        </Header>
    );
}
