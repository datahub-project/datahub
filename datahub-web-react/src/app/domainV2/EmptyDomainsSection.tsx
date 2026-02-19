import { PlusOutlined } from '@ant-design/icons';
import { Button, Empty, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

const EmptyDomainContainer = styled.div`
    display: flex;
    justify-content: center;
    overflow-y: auto;
`;

const StyledEmpty = styled(Empty)`
    width: 35vw;
    @media screen and (max-width: 1300px) {
        width: 50vw;
    }
    @media screen and (max-width: 896px) {
        overflow-y: auto;
        max-height: 75vh;
        &::-webkit-scrollbar {
            width: 5px;
            background: ${(props) => props.theme.colors.bgSurface};
        }
    }
    padding: 20px;
    .ant-empty-image {
        display: none;
    }
`;

const StyledButton = styled(Button)`
    margin: 18px 8px 0 0;
`;

const IconContainer = styled.span`
    color: ${(props) => props.theme.colors.textTertiary};
    font-size: 40px;
`;

interface Props {
    title?: string;
    setIsCreatingDomain: React.Dispatch<React.SetStateAction<boolean>>;
    description?: React.ReactNode;
    icon?: React.ReactNode;
}

function EmptyDomainsSection(props: Props) {
    const { title, description, setIsCreatingDomain, icon } = props;
    return (
        <EmptyDomainContainer>
            <StyledEmpty
                description={
                    <>
                        <IconContainer>{icon}</IconContainer>
                        <Typography.Title level={4}>{title}</Typography.Title>
                        {description}
                    </>
                }
            >
                <StyledButton onClick={() => setIsCreatingDomain(true)}>
                    <PlusOutlined /> Create Domain
                </StyledButton>
            </StyledEmpty>
        </EmptyDomainContainer>
    );
}

export default EmptyDomainsSection;
