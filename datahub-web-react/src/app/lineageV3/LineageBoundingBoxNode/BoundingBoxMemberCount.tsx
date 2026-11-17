import React, { useContext } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { BOUNDING_BOX_MEMBER_PAGE_SIZE, LineageNodesContext } from '@app/lineageV3/common';
import InfoPopover from '@app/sharedV2/icons/InfoPopover';

import { useGetDataProductEntitiesForLineageQuery } from '@graphql/dataProduct.generated';
import { useGetSemanticModelEntitiesForLineageQuery } from '@graphql/semanticModel.generated';
import { EntityType } from '@types';

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

interface Props {
    urn: string;
    /** Number of this container's members currently shown in the box. */
    memberCount: number;
}

/**
 * Member counter shown on the right of a bounding-box header (DataProduct or
 * SemanticModel). The home box shows "x / y entities shown" with a "Show more" control
 * that pages in more members; other boxes show how many of their assets are connected to
 * the home box, and are not paginated.
 */
export default function BoundingBoxMemberCount(props: Props) {
    const { rootType } = useContext(LineageNodesContext);
    switch (rootType) {
        case EntityType.SemanticModel:
            return <SemanticModelBoundingBoxMemberCount {...props} />;
        case EntityType.DataProduct:
        default:
            return <DataProductBoundingBoxMemberCount {...props} />;
    }
}

function DataProductBoundingBoxMemberCount({ urn, memberCount }: Props) {
    const { data } = useGetDataProductEntitiesForLineageQuery({
        variables: { urn, start: 0, count: 0 },
        fetchPolicy: 'cache-first',
    });
    return (
        <BoundingBoxMemberCountView urn={urn} memberCount={memberCount} total={data?.dataProduct?.entities?.total} />
    );
}

function SemanticModelBoundingBoxMemberCount({ urn, memberCount }: Props) {
    const { data } = useGetSemanticModelEntitiesForLineageQuery({
        variables: { urn, start: 0, count: 0 },
        fetchPolicy: 'cache-first',
    });
    return (
        <BoundingBoxMemberCountView urn={urn} memberCount={memberCount} total={data?.semanticModel?.entities?.total} />
    );
}

function BoundingBoxMemberCountView({ urn, memberCount, total }: Props & { total: number | undefined }) {
    const { t } = useTranslation('lineage');
    const { rootUrn, nodes, boundingBoxEntities, setDisplayVersion } = useContext(LineageNodesContext);

    if (total === undefined) return null;

    if (urn === rootUrn) {
        const node = nodes.get(urn);
        const limit = node?.boundingBoxLimit ?? BOUNDING_BOX_MEMBER_PAGE_SIZE;
        // Runs ahead of `limit`: members past the seeded page are displayed when another member's
        // lineage reaches them.
        const shown = Math.min(memberCount, total);
        return (
            <StackedWrapper>
                <span>{t('dataProduct.assetCount', { shown, total })}</span>
                {limit < total && (
                    <ShowMoreButton
                        onClick={() => {
                            if (!node) return;
                            // Mutate + bump displayVersion, as in the lineage filter ShowMoreButton; this
                            // both re-seeds the graph and lets the fetch hook page in more members.
                            node.boundingBoxLimit = limit + BOUNDING_BOX_MEMBER_PAGE_SIZE;
                            setDisplayVersion(([version, urns]) => [version + 1, urns]);
                        }}
                        data-testid="container-show-more"
                    >
                        {t('dataProduct.showMore', { count: BOUNDING_BOX_MEMBER_PAGE_SIZE })}
                    </ShowMoreButton>
                )}
            </StackedWrapper>
        );
    }

    const shown = Math.min(memberCount, total);
    const homeName = nodes.get(rootUrn)?.entity?.name ?? boundingBoxEntities.get(rootUrn)?.name ?? rootUrn;
    return (
        <Wrapper>
            <span>{t('dataProduct.assetCount', { shown, total })}</span>
            <InfoPopover content={t('dataProduct.assetsConnectedToHome', { shown, total, home: homeName })} />
        </Wrapper>
    );
}
