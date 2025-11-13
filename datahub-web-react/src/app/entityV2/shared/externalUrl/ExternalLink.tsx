import LaunchIcon from '@mui/icons-material/Launch';
import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { Popover } from '@src/alchemy-components';
import useMeasureIfTrancated from '@src/app/shared/useMeasureIfTruncated';

const Link = styled.a<{ $isEntityPageHeader?: boolean }>`
    display: flex;
    align-items: center;
    gap: 5px;

    border-radius: 4px;
    padding: ${(props) => (props.$isEntityPageHeader ? '6px' : '4px 6px')};

    background: ${REDESIGN_COLORS.LIGHT_TEXT_DARK_BACKGROUND};
    color: ${(props) => props.theme.styles['primary-color']};

    max-width: 215px;
    width: fit-content;
`;

const LabelWrapper = styled.span`
    text-wrap: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const IconWrapper = styled.span`
    display: flex;
    align-items: center;
    font-size: ${4 / 3}em;
`;

const PopoverWrapper = styled.div`
    max-width: 300px;
    overflow-wrap: break-word;
`;

interface Props {
    href: string;
    label: string;
    onClick?: React.MouseEventHandler<HTMLAnchorElement>;
    className?: string;
    isEntityPageHeader?: boolean;
}

export default function ExternalLink({ href, label, onClick, className, isEntityPageHeader }: Props) {
    const { measuredRef, isHorizontallyTruncated } = useMeasureIfTrancated();

    return (
        <Popover content={isHorizontallyTruncated ? <PopoverWrapper>{label}</PopoverWrapper> : undefined}>
            <Link
                href={href}
                target="_blank"
                onClick={onClick}
                className={className}
                $isEntityPageHeader={isEntityPageHeader}
            >
                <IconWrapper>
                    <LaunchIcon fontSize="inherit" />
                </IconWrapper>
                <LabelWrapper ref={measuredRef}>{label}</LabelWrapper>
            </Link>
        </Popover>
    );
}
