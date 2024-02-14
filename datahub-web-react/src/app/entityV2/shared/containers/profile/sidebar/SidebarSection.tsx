import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';

const Container = styled.div`
    padding-top: 16px;
    padding-bottom: 16px;
    display: flex;
`;

const LeftColumn = styled.div`
    width: 92px;
`;

const RightColumn = styled.div`
    flex: 1;
`;

type Props = {
    title: React.ReactNode;
    content: React.ReactNode;
};

export const SidebarSection = ({ title, content }: Props) => {
    return (
        <Container>
            <LeftColumn>
                <Typography.Text strong>{title}</Typography.Text>
            </LeftColumn>
            <RightColumn>{content}</RightColumn>
        </Container>
    );
};
