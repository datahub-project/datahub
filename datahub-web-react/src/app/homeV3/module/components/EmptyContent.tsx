import { Button, Icon, Text, borders, radius } from '@components';
import { ArrowRight } from '@phosphor-icons/react/dist/csr/ArrowRight';
import React from 'react';
import styled from 'styled-components';

interface Props {
    icon: React.ComponentType<any>;
    title: string;
    description: string;
    linkText?: string;
    linkIcon?: React.ComponentType<any>;
    onLinkClick?: () => void;
}

const Container = styled.div`
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;

    p {
        text-align: center;
        width: 80%;
    }
`;

const IconWrapper = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;

    width: 32px;
    height: 32px;

    border: ${borders['1px']} ${(props) => props.theme.colors.border};
    border-radius: ${radius.full};
    margin-bottom: 8px;
    color: ${(props) => props.theme.colors.icon};
`;

const Title = styled(Text).attrs({ size: 'lg', weight: 'bold' })`
    color: ${(props) => props.theme.colors.text};
`;

const Description = styled(Text)`
    color: ${(props) => props.theme.colors.textSecondary};
`;

export default function EmptyContent({ icon, title, description, linkText, linkIcon, onLinkClick }: Props) {
    return (
        <Container>
            <IconWrapper>
                <Icon icon={icon} color="icon" />
            </IconWrapper>
            <Title>{title}</Title>
            <Description>{description}</Description>
            {linkText && onLinkClick && (
                <Button variant="text" onClick={onLinkClick}>
                    {linkText} <Icon icon={linkIcon ?? ArrowRight} color="primary" size="md" />
                </Button>
            )}
        </Container>
    );
}
