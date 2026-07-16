import { Icon, Popover } from '@components';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import React from 'react';
import styled from 'styled-components';

const InfoWrapper = styled.div<{ $iconColor?: string }>`
    display: inline-flex;
    align-items: center;
    color: ${({ theme, $iconColor }) => $iconColor || theme.colors.iconBrand};
`;

interface Props {
    content: React.ReactNode;
    className?: string;
    iconColor?: string;
}

export default function InfoPopover({ content, className, iconColor }: Props) {
    return (
        <InfoWrapper className={className} $iconColor={iconColor}>
            <Popover placement="top" content={content} trigger="hover" showArrow={false}>
                <Icon icon={Info} size="sm" weight="regular" color="inherit" />
            </Popover>
        </InfoWrapper>
    );
}
