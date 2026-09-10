import React from 'react';
import styled, { useTheme } from 'styled-components';

import { NameContainer } from '@app/homeV3/styledComponents';

const SecondaryTextContainer = styled(NameContainer)`
    font-weight: 400;
    font-size: 12px;
    line-height: 16px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const EllipsisText = styled.span<{ $ellipsis?: boolean }>`
    font-size: 12px;
    line-height: 16px;
    color: ${(props) => props.theme.colors.textSecondary};
    white-space: nowrap;

    ${(props) =>
        props.$ellipsis &&
        `
            display: block;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 100%;
        `}
`;

type Props = {
    children: React.ReactNode;
    className?: string;
    ellipsis?: boolean;
    showTooltipIfTruncated?: boolean;
};

export default function ModuleSecondaryText({ children, className, ellipsis, showTooltipIfTruncated }: Props) {
    const theme = useTheme();

    if (ellipsis && showTooltipIfTruncated) {
        return (
            <SecondaryTextContainer
                className={className}
                ellipsis={{
                    tooltip: {
                        overlayInnerStyle: { color: theme.colors.textSecondary },
                        showArrow: false,
                    },
                }}
            >
                {children}
            </SecondaryTextContainer>
        );
    }

    return (
        <EllipsisText className={className} $ellipsis={ellipsis}>
            {children}
        </EllipsisText>
    );
}
