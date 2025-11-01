import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { Text, Heading, Pill, Icon } from '@components';

// Styled Components
const BreadcrumbWrapper = styled.div`
    padding: 20px 24px 16px 24px;
    background-color: white;
`;

const BreadcrumbContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 0 0 16px 0;
    &&& {
        font-size: 16px;
    }
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
`;

// Breadcrumb Item Props
interface BreadcrumbItem {
    label: string;
    href?: string;
    isActive?: boolean;
}

interface BreadcrumbHeaderProps {
    items: BreadcrumbItem[];
    title: string;
    subtitle?: string;
    pillLabel?: string;
    pillColor?: string;
}

// Main Breadcrumb Component
export const BreadcrumbHeader: React.FC<BreadcrumbHeaderProps> = ({
    items,
    title,
    subtitle,
    pillLabel,
    pillColor = 'blue'
}) => {
    return (
        <BreadcrumbWrapper>
            <BreadcrumbContainer>
                {items.map((item, index) => (
                    <React.Fragment key={index}>
                        {item.href && !item.isActive ? (
                            <Link to={item.href}>
                                <Text size="lg" color="gray">
                                    {item.label}
                                </Text>
                            </Link>
                        ) : (
                            <Text size="lg" color="gray">
                                {item.label}
                            </Text>
                        )}
                        {index < items.length - 1 && (
                            <Icon icon="CaretRight" source="phosphor" size="md" color="gray" />
                        )}
                    </React.Fragment>
                ))}
            </BreadcrumbContainer>
            <Header>
                <Heading type="h2" size="2xl" weight="bold">
                    {title}
                </Heading>
                {pillLabel && (
                    <Pill label={pillLabel} color={pillColor} size="sm" />
                )}
            </Header>
            {subtitle && (
                <Text size="md" color="gray">
                    {subtitle}
                </Text>
            )}
        </BreadcrumbWrapper>
    );
};

export default BreadcrumbHeader;
