import React from 'react';
import styled from 'styled-components/macro';
import { useHistory } from 'react-router';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { SidebarSection } from '../SidebarSection';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { navigateToDomainDataProducts } from './utils';
import { pluralize } from '../../../../../../shared/textUtil';
import EmptySectionText from '../EmptySectionText';
import { REDESIGN_COLORS } from '../../../../constants';

const Section = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    flex-wrap: wrap;
    text-wrap: wrap;
`;

const SummaryText = styled.div``;

const ViewAllButton = styled.div`
    display: flex;
    align-items: center;
    font-weight: bold;
    padding: 0px 2px;
    margin-left: 8px;
    color: ${REDESIGN_COLORS.DARK_GREY};
    :hover {
        cursor: pointer;
    }
`;

const SidebarDataProductsSection = () => {
    const { urn, entityType, entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const history = useHistory();

    const domain = entityData as any;
    const productsCount = domain?.dataProducts?.total || 0;
    const hasProducts = productsCount > 0;

    if (!hasProducts) {
        return null;
    }

    return (
        <SidebarSection
            title="Data Products"
            key="Data Products"
            content={
                <>
                    {(hasProducts && (
                        <>
                            <Section>
                                <SummaryText>
                                    {productsCount}
                                    {pluralize(productsCount, 'data product')}
                                </SummaryText>
                                <ViewAllButton
                                    onClick={() =>
                                        navigateToDomainDataProducts(urn, entityType, history, entityRegistry)
                                    }
                                >
                                    View all
                                </ViewAllButton>
                            </Section>
                        </>
                    )) || <EmptySectionText message="No products yet" />}
                </>
            }
        />
    );
};

export default SidebarDataProductsSection;
