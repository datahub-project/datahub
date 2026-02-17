import { Button, Icon, Text, borders, radius } from '@components';
import React from 'react';
import styled from 'styled-components';

interface Props {
    icon: string;
    title: string;
    description: string;
    linkText?: string;
    linkIcon?: string;
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
`;

export default function EmptyContent({ icon, title, description, linkText, linkIcon, onLinkClick }: Props) {
    return (
        <Container>
            <IconWrapper>
                <Icon icon={icon} source="phosphor" />
            </IconWrapper>
            <Text size="lg" weight="bold">
                {title}
            </Text>
            <Text>{description}</Text>
            {linkText && onLinkClick && (
                <Button variant="text" onClick={onLinkClick}>
                    {linkText} <Icon icon={linkIcon ?? 'ArrowRight'} color="primary" source="phosphor" size="md" />
                </Button>
            )}
        </Container>
    );
}
