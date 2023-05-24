import React, { ReactNode } from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../entity/shared/constants';

const Layout = styled.div`
    margin-left: 8px;
`;

const HeaderContainer = styled.div``;

const BodyGridExpander = styled.div<{ isOpen: boolean }>`
    display: grid;
    grid-template-rows: ${(props) => (props.isOpen ? '1fr' : '0fr')};
    transition: grid-template-rows 0.2s;
    overflow: hidden;
`;

const BodyContainer = styled.div`
    min-height: 0;
`;

type ExpandableNodeProps = {
    isOpen: boolean;
    header: ReactNode;
    body: ReactNode;
};

const ExpandableNode = ({ isOpen, header, body }: ExpandableNodeProps) => {
    return (
        <Layout>
            <HeaderContainer>{header}</HeaderContainer>
            <BodyGridExpander isOpen={isOpen}>
                <BodyContainer>{body}</BodyContainer>
            </BodyGridExpander>
        </Layout>
    );
};

ExpandableNode.Header = styled.div<{ isOpen: boolean; showBorder?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    cursor: pointer;
    user-select: none;
    padding-top: 8px;
    border-bottom: ${(props) => `1px solid ${props.isOpen || !props.showBorder ? 'none' : ANTD_GRAY[4]}`};
`;

ExpandableNode.HeaderLeft = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

ExpandableNode.Body = styled.div``;

export default ExpandableNode;
