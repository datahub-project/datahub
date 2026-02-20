import { Divider } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { formatNumber } from '@app/shared/formatNumber';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { Domain, EntityType } from '@types';

const Card = styled(Link)<{ $isShowNavBarRedesign?: boolean }>`
    border-radius: ${(props) => (props.$isShowNavBarRedesign ? '8px' : '10px')};
    background-color: ${(props) => props.theme.colors.bgSurface};
    padding: 16px;
    border: ${(props) =>
        props.$isShowNavBarRedesign ? `1px solid ${props.theme.colors.border}` : '2px solid transparent'};

    :hover {
        border: ${(props) => (props.$isShowNavBarRedesign ? '1px' : '2px')} solid
            ${(props) => props.theme.colors.hyperlinks};
    }

    display: flex;
    justify-content: start;
    align-items: center;
    max-width: 428px;
`;

const Text = styled.div`
    margin-left: 12px;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
`;

const Name = styled.div`
    font-size: 16px;
    color: ${(props) => props.theme.colors.text};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 150px;
`;

const Counts = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
`;

const Count = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textTertiary};
    overflow: hidden;
    text-overflow: ellipsis;
`;

type Props = {
    domain: Domain;
    assetCount: number;
};

export const DomainCard = ({ domain, assetCount }: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const entityRegistry = useEntityRegistry();
    const name = entityRegistry.getDisplayName(EntityType.Domain, domain);
    const dataProductCount = (domain as any).dataProducts?.total || 0;

    return (
        <HoverEntityTooltip placement="bottom" showArrow={false} entity={domain}>
            <Card
                key={domain.urn}
                to={entityRegistry.getEntityUrl(domain.type, domain.urn)}
                $isShowNavBarRedesign={isShowNavBarRedesign}
            >
                <DomainColoredIcon domain={domain} size={46} />
                <Text>
                    <Name>{name}</Name>
                    <Counts>
                        <Count>{formatNumber(assetCount)} assets</Count>
                        <Divider type="vertical" />
                        <Count>{formatNumber(dataProductCount)} data products</Count>
                    </Counts>
                </Text>
            </Card>
        </HoverEntityTooltip>
    );
};
