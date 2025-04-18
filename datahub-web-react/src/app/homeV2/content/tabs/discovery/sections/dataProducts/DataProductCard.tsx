import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import { colors } from '@src/alchemy-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { DataProduct, Domain, EntityType } from '../../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { ANTD_GRAY } from '../../../../../../entity/shared/constants';
import { DomainColoredIcon } from '../../../../../../entityV2/shared/links/DomainColoredIcon';
import { HoverEntityTooltip } from '../../../../../../recommendations/renderer/component/HoverEntityTooltip';
import { SEARCH_COLORS } from '../../../../../../entityV2/shared/constants';

const Card = styled(Link)<{ $isShowNavBarRedesign?: boolean }>`
    border-radius: ${(props) => (props.$isShowNavBarRedesign ? '8px' : '10px')};
    background-color: #ffffff;
    padding: 10px 16px;
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

const Section = styled.div`
    margin-top: 5px;
    display: flex;
    align-items: center;
    justify-content: start;
`;

const SectionName = styled.div`
    font-size: 14px;
    margin-left: 10px;
    color: ${ANTD_GRAY[7]};
    overflow: hidden;
    text-overflow: ellipsis;
`;

type Props = {
    dataProduct: DataProduct;
    domain: Domain;
};

export const DataProductCard = ({ dataProduct, domain }: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const entityRegistry = useEntityRegistry();
    const domainName = entityRegistry.getDisplayName(EntityType.Domain, domain);

    return (
        <HoverEntityTooltip placement="bottom" showArrow={false} entity={dataProduct}>
            <Card
                key={dataProduct.urn}
                to={entityRegistry.getEntityUrl(dataProduct.type, dataProduct.urn)}
                $isShowNavBarRedesign={isShowNavBarRedesign}
            >
                <Text>
                    <Name>{dataProduct.properties?.name}</Name>
                    <Section>
                        <DomainColoredIcon domain={domain} size={26} />
                        <SectionName>{domainName}</SectionName>
                    </Section>
                </Text>
            </Card>
        </HoverEntityTooltip>
    );
};
