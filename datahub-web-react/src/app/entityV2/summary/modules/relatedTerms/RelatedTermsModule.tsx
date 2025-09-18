import { Text } from '@components';
import React from 'react';
import { useHistory } from 'react-router';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { RelatedTermTypes } from '@app/entityV2/glossaryTerm/profile/GlossaryRelatedTermsResult';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import { ModuleProps } from '@app/homeV3/module/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useGetRelatedTermsQuery } from '@graphql/glossary.generated';
import { DataHubPageModuleType } from '@types';

export default function RelatedTermsModule(props: ModuleProps) {
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

    let hasData = false;
    Object.keys(RelatedTermTypes).forEach((relationshipType) => {
        if (data?.glossaryTerm?.[relationshipType]?.relationships?.length) {
            hasData = true;
        }
    });

    return (
        <LargeModule
            {...props}
            loading={loading}
            onClickViewAll={navigateToRelatedTermsTab}
            dataTestId="related-terms-module"
        >
            {!hasData && (
                <EmptyContent
                    icon="BookmarkSimple"
                    title="No Related Terms"
                    description="Add relationship for this glossary term to see them in this list"
                    linkText="Add related terms"
                    onLinkClick={navigateToRelatedTermsTab}
                />
            )}
            {hasData && (
                <>
                    {Object.keys(RelatedTermTypes).map((relationshipType) => {
                        const relatedTerms = data?.glossaryTerm?.[relationshipType]?.relationships || [];
                        return relatedTerms
                            .filter((relationship) => !!relationship.entity)
                            .map((relationship) => (
                                <EntityItem
                                    entity={relationship.entity}
                                    key={relationship.entity?.urn}
                                    moduleType={DataHubPageModuleType.RelatedTerms}
                                    customDetailsRenderer={() => (
                                        <Text size="sm" color="gray">
                                            {RelatedTermTypes[relationshipType]}
                                        </Text>
                                    )}
                                />
                            ));
                    })}
                </>
            )}
        </LargeModule>
    );
}
