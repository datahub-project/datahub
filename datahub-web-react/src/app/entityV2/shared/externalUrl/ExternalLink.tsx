import React from 'react';
import LaunchIcon from '@mui/icons-material/Launch';
import styled from 'styled-components';
import { colors, Popover } from '@src/alchemy-components';
import useMeasureIfTrancated from '@src/app/shared/useMeasureIfTruncated';
import { REDESIGN_COLORS } from '../constants';

const Link = styled.a`
    display: flex;
    align-items: center;
    gap: 5px;

    border-radius: 4px;
    padding: 4px 6px;

    background: ${REDESIGN_COLORS.LIGHT_TEXT_DARK_BACKGROUND};
    color: ${colors.violet[500]};

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
`;

interface Props {
    href: string;
    label: string;
    onClick?: React.MouseEventHandler<HTMLAnchorElement>;
    className?: string;
}

export default function ExternalLink({ href, label, onClick, className }: Props) {
    const { measuredRef, isHorizontallyTruncated } = useMeasureIfTrancated();

    return (
        <Popover content={isHorizontallyTruncated ? <PopoverWrapper>{label}</PopoverWrapper> : undefined}>
            <Link href={href} target="_blank" onClick={onClick} className={className}>
                <IconWrapper>
                    <LaunchIcon fontSize="inherit" />
                </IconWrapper>
                <LabelWrapper ref={measuredRef}>{label}</LabelWrapper>
            </Link>
        </Popover>
    );
}
