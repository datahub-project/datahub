import React, { ReactNode, memo } from 'react';
import styled from 'styled-components';

const Layout = styled.div<{ depth: number }>`
    margin-left: ${(props) => props.depth * 8}px;
`;
const HeaderContainer = styled.div``;

const BodyGridExpander = styled.div<{ isOpen: boolean }>`
    display: grid;
    grid-template-rows: ${(props) => (props.isOpen ? '1fr' : '0fr')};
    transition: grid-template-rows 0.2s;
    /* overflow: scroll; */
    overflow: hidden;
`;

const BodyContainer = styled.div`
    min-height: 0;
`;

type Props = {
    isOpen: boolean;
    depth?: number;
    header: ReactNode;
    body: ReactNode;
};

const ExpandableNode = ({ isOpen, depth = 0, header, body }: Props) => {
    return (
        <Layout depth={depth}>
            <HeaderContainer>{header}</HeaderContainer>
            <BodyGridExpander isOpen={isOpen}>
                <BodyContainer>{body}</BodyContainer>
            </BodyGridExpander>
        </Layout>
    );
};

export default memo(ExpandableNode);
