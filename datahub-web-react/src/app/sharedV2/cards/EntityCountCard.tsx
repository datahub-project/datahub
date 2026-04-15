import { Tooltip } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { generateColor } from '@app/entityV2/shared/components/styled/StyledTag';
import { formatNumber, formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';

const Card = styled(Link)`
    align-items: center;
    background-color: ${(props) => props.theme.colors.bg};
    border: 1.5px solid ${(props) => props.theme.colors.border};
    border-radius: 10px;
    display: flex;
    justify-content: start;
    min-width: 180px;
    padding: 16px;
    :hover {
        border: 1.5px solid ${(props) => props.theme.colors.borderInformation};
        cursor: pointer;
    }
`;

const Text = styled.div`
    overflow: hidden;
`;

const Name = styled.div`
    font-size: 16px;
    color: ${(props) => props.theme.colors.textTertiary};
    overflow: hidden;
    text-overflow: ellipsis;
    text-transform: capitalize;
    max-width: 160px;
    white-space: nowrap;
`;

const IconWrapper = styled.div<{ color?: string; backgroundColor?: string }>`
    align-items: center;
    border-radius: 12px;
    background-color: ${(props) => props.backgroundColor || props.theme.colors.bgSurface};
    display: flex;
    height: 50px;
    justify-content: center;
    margin-right: 14px;
    min-width: 50px;
    padding: 6px;

    && {
        font-size: 30px;
    }
`;

const Count = styled.div`
    font-size: 20px;
    color: ${(props) => props.theme.colors.text};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

type Props = {
    icon: JSX.Element;
    type: string;
    name: string;
    tooltipDescriptor?: string;
    count: number;
    link: string;
};

export function EntityCountCard({ icon, name, type, tooltipDescriptor, count, link }: Props) {
    const card = (
        <Card to={link}>
            <IconWrapper backgroundColor={`${generateColor.hex(type)}30`}>{icon}</IconWrapper>
            <Text>
                <Name>{name}</Name>
                {(count !== undefined && <Count>{formatNumber(count)}</Count>) || null}
            </Text>
        </Card>
    );

    if (tooltipDescriptor) {
        return (
            <Tooltip
                title={`View ${formatNumberWithoutAbbreviation(count)} ${tooltipDescriptor}`}
                showArrow={false}
                placement="bottom"
            >
                {card}
            </Tooltip>
        );
    }
    return card;
}
