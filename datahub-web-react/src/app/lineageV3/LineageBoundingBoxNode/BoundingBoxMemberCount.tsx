import React, { useContext } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { DATA_PRODUCT_MEMBER_PAGE_SIZE, LineageNodesContext } from '@app/lineageV3/common';
import InfoPopover from '@app/sharedV2/icons/InfoPopover';

import { useGetDataProductEntitiesForLineageQuery } from '@graphql/dataProduct.generated';

const Wrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    line-height: 1.2;
    white-space: nowrap;
    color: ${(props) => props.theme.colors.textSecondary};
`;

// Home box: stack the "x / y assets" count and the "Show more" control vertically, right-aligned.
const StackedWrapper = styled(Wrapper)`
    flex-direction: column;
    align-items: flex-end;
    gap: 2px;
`;

const ShowMoreButton = styled.button`
    all: unset;
    cursor: pointer;
    border-radius: 12px;
    padding: 2px 8px;
    font-size: 12px;
    color: ${(props) => props.theme.colors.textBrand};

    :hover {
        background-color: ${(props) => props.theme.colors.bgSurfaceBrand};
    }
`;

/** Fetches a data product's total member count. Cache-first so it hits the network once per product. */
function useDataProductMemberTotal(urn: string): number | undefined {
    const { data } = useGetDataProductEntitiesForLineageQuery({
        variables: { urn, start: 0, count: 0 },
        fetchPolicy: 'cache-first',
    });
    return data?.dataProduct?.entities?.total ?? undefined;
}

interface Props {
    urn: string;
    /** Number of this product's members currently shown in the box. */
    memberCount: number;
}

/**
 * Member counter shown on the right of a data product bounding box header. The home product shows
 * "x / y entities shown" with a "Show more" control that pages in more members; other products show
 * how many of their assets are connected to the home product, and are not paginated.
 */
export default function BoundingBoxMemberCount({ urn, memberCount }: Props) {
    const { t } = useTranslation('lineage');
    const { rootUrn, nodes, dataProductEntities, setDisplayVersion } = useContext(LineageNodesContext);
    const total = useDataProductMemberTotal(urn);

    if (total === undefined) return null;

    if (urn === rootUrn) {
        const node = nodes.get(urn);
        const limit = node?.boundingBoxLimit ?? DATA_PRODUCT_MEMBER_PAGE_SIZE;
        // "Shown" tracks pagination progress, not raw displayed nodes (which include sibling copies
        // and so can exceed the product's entity count).
        const shown = Math.min(limit, total);
        return (
            <StackedWrapper>
                <span>{t('dataProduct.assetCount', { shown, total })}</span>
                {limit < total && (
                    <ShowMoreButton
                        onClick={() => {
                            if (!node) return;
                            // Mutate + bump displayVersion, as in the lineage filter ShowMoreButton; this
                            // both re-seeds the graph and lets the fetch hook page in more members.
                            node.boundingBoxLimit = limit + DATA_PRODUCT_MEMBER_PAGE_SIZE;
                            setDisplayVersion(([version, urns]) => [version + 1, urns]);
                        }}
                        data-testid="data-product-show-more"
                    >
                        {t('dataProduct.showMore', { count: DATA_PRODUCT_MEMBER_PAGE_SIZE })}
                    </ShowMoreButton>
                )}
            </StackedWrapper>
        );
    }

    const shown = Math.min(memberCount, total);
    const homeName = nodes.get(rootUrn)?.entity?.name ?? dataProductEntities.get(rootUrn)?.name ?? rootUrn;
    return (
        <Wrapper>
            <span>{t('dataProduct.assetCount', { shown, total })}</span>
            <InfoPopover content={t('dataProduct.assetsConnectedToHome', { shown, total, home: homeName })} />
        </Wrapper>
    );
}
