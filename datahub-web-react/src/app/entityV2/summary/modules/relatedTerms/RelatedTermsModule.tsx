import { Text } from '@components';
import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';

import { useEntityData } from '@app/entity/shared/EntityContext';
import {
    RelatedTermTypes,
    getRelatedTermTypeLabel,
} from '@app/entityV2/glossaryTerm/profile/GlossaryRelatedTermsResult';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import { ModuleProps } from '@app/homeV3/module/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useGetRelatedTermsQuery } from '@graphql/glossary.generated';
import { DataHubPageModuleType } from '@types';

// Maps each RelatedTermTypes enum key to the query alias(es) that back it.
// Merged bidirectional types pull from two aliases and deduplicate by URN.
const RELATIONSHIP_ALIASES: Record<string, string[]> = {
    isRelatedTerms: ['isRelatedTerms'],
    hasRelatedTerms: ['hasRelatedTerms'],
    isAChildren: ['isAChildren'],
    containedBy: ['containedBy'],
    relatedTermsMerged: ['relatedTo', 'relatedFrom'],
    synonymsMerged: ['synonymOf', 'synonymWith'],
    antonymsMerged: ['antonymOf', 'antonymWith'],
    translatesTo: ['translatesTo'],
    translatedFrom: ['translatedFrom'],
    hasValue: ['hasValue'],
    isValueOf: ['isValueOf'],
};

function getRelationshipsForType(
    glossaryTerm: Record<string, any> | undefined | null,
    typeKey: string,
): Array<{ entity: any }> {
    const aliases = RELATIONSHIP_ALIASES[typeKey] || [];
    const seen = new Set<string>();
    return aliases
        .flatMap((alias) => (glossaryTerm?.[alias]?.relationships || []) as Array<{ entity: any }>)
        .filter((rel) => {
            const urn = rel?.entity?.urn;
            if (!urn || seen.has(urn)) return false;
            seen.add(urn);
            return true;
        });
}

export default function RelatedTermsModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const entityRegistry = useEntityRegistryV2();
    const history = useHistory();
    const { entityType, urn } = useEntityData();
    const { isReloading, onReloadingFinished } = useModuleContext();
    const { data, loading } = useGetRelatedTermsQuery({
        variables: { urn },
        skip: !urn,
        fetchPolicy: isReloading ? 'cache-and-network' : 'cache-first',
        onCompleted: () => onReloadingFinished?.(),
    });

    const navigateToRelatedTermsTab = () => {
        history.push(`${entityRegistry.getEntityUrl(entityType, urn)}/Related Terms`);
    };

    const glossaryTerm = data?.glossaryTerm as Record<string, any> | undefined | null;

    // Collect all non-empty relationship entries with their display label
    const allEntries: Array<{ entity: any; typeLabel: string }> = [];
    Object.keys(RelatedTermTypes).forEach((typeKey) => {
        const rels = getRelationshipsForType(glossaryTerm, typeKey);
        rels.filter((r) => !!r.entity).forEach((r) => {
            allEntries.push({ entity: r.entity, typeLabel: RelatedTermTypes[typeKey] });
        });
    });

    const hasData = allEntries.length > 0;

    return (
        <LargeModule
            {...props}
            loading={loading}
            onClickViewAll={navigateToRelatedTermsTab}
            dataTestId="related-terms-module"
        >
            {!hasData && (
                <EmptyContent
                    icon={BookmarkSimple}
                    title={t('relatedTerms.emptyTitle')}
                    description={t('relatedTerms.emptyDescription')}
                    linkText={t('relatedTerms.emptyLink')}
                    onLinkClick={navigateToRelatedTermsTab}
                />
            )}
            {hasData &&
                allEntries.map(({ entity, typeLabel }) => (
                    <EntityItem
                        entity={entity}
                        key={`${typeLabel}-${entity.urn}`}
                        moduleType={DataHubPageModuleType.RelatedTerms}
                        customDetailsRenderer={() => <Text size="sm">{getRelatedTermTypeLabel(typeLabel)}</Text>}
                    />
                ))}
        </LargeModule>
    );
}
