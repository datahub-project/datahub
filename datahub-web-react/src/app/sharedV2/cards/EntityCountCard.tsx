import React from 'react';
import styled from 'styled-components/macro';
import { Link } from 'react-router-dom';
import { Tooltip } from '@components';
import { ANTD_GRAY, ANTD_GRAY_V2, REDESIGN_COLORS } from '../../entityV2/shared/constants';
import { formatNumber, formatNumberWithoutAbbreviation } from '../../shared/formatNumber';
import { generateColor } from '../../entityV2/shared/components/styled/StyledTag';

const Card = styled(Link)`
    align-items: center;
    background-color: ${ANTD_GRAY[1]};
    border: 1.5px solid ${ANTD_GRAY_V2[5]};
    border-radius: 10px;
    display: flex;
    justify-content: start;
    min-width: 180px;
    padding: 16px;
    :hover {
        border: 1.5px solid ${REDESIGN_COLORS.BLUE};
        cursor: pointer;
    }
`;

const Text = styled.div`
    overflow: hidden;
`;

const Name = styled.div`
    font-size: 16px;
    color: ${ANTD_GRAY[7]};
    overflow: hidden;
    text-overflow: ellipsis;
    text-transform: capitalize;
    max-width: 160px;
    white-space: nowrap;
`;

const IconWrapper = styled.div<{ color?: string; backgroundColor?: string }>`
    align-items: center;
    border-radius: 12px;
    background-color: ${({ backgroundColor }) => backgroundColor || ANTD_GRAY[3]};
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
    color: ${ANTD_GRAY[10]};
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
