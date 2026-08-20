import { Pill, borders, radius } from '@components';
import { Storefront } from '@phosphor-icons/react/dist/csr/Storefront';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { OwnerAvatarGroup } from '@app/sharedV2/owners/OwnerAvatarGroup';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

import { GetRootDataProductsBrowseQuery } from '@graphql/marketplaceBrowse.generated';
import { EntityType } from '@types';

const MAX_TAGS = 3;

type DataProduct = NonNullable<
    NonNullable<GetRootDataProductsBrowseQuery['getRootDataProducts']>['dataProducts'][number]
>;

type Props = {
    dataProduct: DataProduct;
};

const CardLink = styled(Link)`
    display: flex;
    flex-direction: column;
    gap: 12px;
    padding: 16px 18px;
    background: ${(props) => props.theme.colors.bg};
    border: ${borders['1px']} ${(props) => props.theme.colors.border};
    border-radius: ${radius.lg};
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    color: inherit;
    transition:
        border-color 0.15s ease,
        box-shadow 0.15s ease;

    &:hover {
        border-color: ${(props) => props.theme.colors.borderHover};
        box-shadow: ${(props) => props.theme.colors.shadowSm};
        color: inherit;
    }
`;

const TitleRow = styled.div`
    display: flex;
    align-items: flex-start;
    gap: 10px;
`;

const IconWrap = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    margin-top: 2px;
    color: ${(props) => props.theme.colors.iconBrand};
`;

const TitleBlock = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    min-width: 0;
`;

const Title = styled.div`
    font-size: 16px;
    font-weight: 700;
    color: ${(props) => props.theme.colors.text};
    line-height: 1.35;
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
`;

const DomainLabel = styled.div`
    font-size: 13px;
    font-weight: 500;
    color: ${(props) => props.theme.colors.textSecondary};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const Description = styled.div`
    font-size: 13px;
    line-height: 1.5;
    color: ${(props) => props.theme.colors.textSecondary};
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 3;
    -webkit-box-orient: vertical;
`;

const TagsRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
`;

const Footer = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding-top: 4px;
`;

const Meta = styled.div`
    font-size: 12px;
    color: ${(props) => props.theme.colors.textTertiary};
    white-space: nowrap;
    margin-left: auto;
`;

export default function MarketplaceDataProductCard({ dataProduct }: Props) {
    const { t } = useTranslation('misc');
    const entityRegistry = useEntityRegistryV2();

    const name = dataProduct.properties?.name ?? dataProduct.urn;
    const description = dataProduct.properties?.description?.trim() || null;
    const domain = dataProduct.domain?.domain;
    const domainName = domain ? entityRegistry.getDisplayName(EntityType.Domain, domain) : null;
    const isDeprecated = !!dataProduct.deprecation?.deprecated;
    const numAssets = dataProduct.properties?.numAssets ?? 0;
    const numSubProducts = dataProduct.childDataProducts?.total ?? 0;
    const owners = dataProduct.ownership?.owners ?? [];
    const tags = useMemo(
        () => (dataProduct.tags?.tags ?? []).slice(0, MAX_TAGS).map((tagAssoc) => tagAssoc.tag),
        [dataProduct.tags?.tags],
    );

    const assetsLabel = t('marketplace.cardAssets', { count: numAssets });
    const metaLabel =
        numSubProducts > 0
            ? t('marketplace.cardMetaWithSubProducts', {
                  assets: assetsLabel,
                  subProducts: t('marketplace.cardSubProducts', { count: numSubProducts }),
              })
            : assetsLabel;

    return (
        <CardLink
            to={`${PageRoutes.DATA_PRODUCT_ENTITY}/${encodeURIComponent(dataProduct.urn)}`}
            data-testid={`marketplace-product-card-${dataProduct.urn}`}
        >
            {isDeprecated && (
                <Pill label={t('marketplace.cardDeprecated')} size="sm" color="yellow" clickable={false} />
            )}

            <TitleRow>
                <IconWrap>
                    <Storefront size={18} weight="regular" />
                </IconWrap>
                <TitleBlock>
                    <Title title={name}>{name}</Title>
                    {domainName && <DomainLabel title={domainName}>{domainName}</DomainLabel>}
                </TitleBlock>
            </TitleRow>

            {description && <Description title={description}>{description}</Description>}

            {tags.length > 0 && (
                <TagsRow>
                    {tags.map((tag) => (
                        <Pill
                            key={tag.urn}
                            label={entityRegistry.getDisplayName(EntityType.Tag, tag)}
                            size="sm"
                            color="gray"
                            clickable={false}
                        />
                    ))}
                </TagsRow>
            )}

            <Footer>
                {owners.length > 0 && <OwnerAvatarGroup owners={owners} entityRegistry={entityRegistry} hideLink />}
                <Meta>{metaLabel}</Meta>
            </Footer>
        </CardLink>
    );
}
