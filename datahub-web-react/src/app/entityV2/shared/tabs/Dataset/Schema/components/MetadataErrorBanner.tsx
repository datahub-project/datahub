import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const Banner = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 4px 16px;
    font-size: 12px;
    color: ${(props) => props.theme.colors.textDisabled};
    background: ${(props) => props.theme.colors.bgSurface};
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const RetryLink = styled.button`
    background: none;
    border: none;
    padding: 0;
    font-size: inherit;
    color: ${(props) => props.theme.colors.hyperlinks};
    cursor: pointer;
    text-decoration: underline;
`;

type Props = {
    message: string;
    onRetry: () => void;
};

/**
 * Inline error strip with a retry action for a failed schema query, shared by the full
 * Schema tab and the compact sidebar rendering so the two modes cannot drift apart.
 */
export default function MetadataErrorBanner({ message, onRetry }: Props) {
    const { t: ta } = useTranslation('common.actions');
    return (
        <Banner data-testid="metadata-error-banner">
            {message}{' '}
            <RetryLink type="button" onClick={onRetry}>
                {ta('retry')}
            </RetryLink>
        </Banner>
    );
}
