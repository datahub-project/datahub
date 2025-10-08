import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

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

// Simple Text Component (replace with your preferred text component)
const Text = styled.span<{ size?: string; color?: string; weight?: string }>`
    font-size: ${props => {
        switch (props.size) {
            case 'lg': return '16px';
            case '2xl': return '24px';
            case 'md': return '14px';
            default: return '14px';
        }
    }};
    color: ${props => {
        switch (props.color) {
            case 'gray': return '#6B7280';
            default: return '#000000';
        }
    }};
    font-weight: ${props => {
        switch (props.weight) {
            case 'bold': return '700';
            default: return '400';
        }
    }};
`;

// Simple Icon Component (replace with your preferred icon component)
const Icon = styled.span<{ icon?: string; size?: string; color?: string }>`
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: ${props => {
        switch (props.size) {
            case 'xl': return '20px';
            default: return '16px';
        }
    }};
    color: ${props => {
        switch (props.color) {
            case 'gray': return '#6B7280';
            default: return '#000000';
        }
    }};
`;

// Simple Pill Component (replace with your preferred pill component)
const Pill = styled.span<{ size?: string; label?: string; color?: string }>`
    display: inline-flex;
    align-items: center;
    padding: 4px 8px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 500;
    background-color: ${props => {
        switch (props.color) {
            case 'green': return '#D1FAE5';
            case 'yellow': return '#FEF3C7';
            case 'red': return '#FEE2E2';
            case 'blue': return '#DBEAFE';
            default: return '#F3F4F6';
        }
    }};
    color: ${props => {
        switch (props.color) {
            case 'green': return '#065F46';
            case 'yellow': return '#92400E';
            case 'red': return '#991B1B';
            case 'blue': return '#1E40AF';
            default: return '#374151';
        }
    }};
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
                            <Icon icon="chevron-right" size="xl" color="gray">
                                ›
                            </Icon>
                        )}
                    </React.Fragment>
                ))}
            </BreadcrumbContainer>
            <Header>
                <Text size="2xl" weight="bold">
                    {title}
                </Text>
                {pillLabel && (
                    <Pill size="sm" label={pillLabel} color={pillColor}>
                        {pillLabel}
                    </Pill>
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
