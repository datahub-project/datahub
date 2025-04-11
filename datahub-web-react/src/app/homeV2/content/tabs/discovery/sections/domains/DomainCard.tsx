import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import { Divider } from 'antd';
import { colors } from '@src/alchemy-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { Domain, EntityType } from '../../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { ANTD_GRAY } from '../../../../../../entity/shared/constants';
import { formatNumber } from '../../../../../../shared/formatNumber';
import { DomainColoredIcon } from '../../../../../../entityV2/shared/links/DomainColoredIcon';
import { HoverEntityTooltip } from '../../../../../../recommendations/renderer/component/HoverEntityTooltip';
import { SEARCH_COLORS } from '../../../../../../entityV2/shared/constants';

const Card = styled(Link)<{ $isShowNavBarRedesign?: boolean }>`
    border-radius: ${(props) => (props.$isShowNavBarRedesign ? '8px' : '10px')};
    background-color: #ffffff;
    padding: 16px;
    border: ${(props) => (props.$isShowNavBarRedesign ? `1px solid ${colors.gray[100]}` : '2px solid transparent')};

    :hover {
        border: ${(props) => (props.$isShowNavBarRedesign ? '1px' : '2px')} solid ${SEARCH_COLORS.LINK_BLUE};
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
    color: ${ANTD_GRAY[9]};
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
    color: ${ANTD_GRAY[7]};
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
