import { BookOutlined } from '@ant-design/icons';
import React from 'react';

import { IconStyleType, PreviewType } from '@app/entity/Entity';
import { getRelatedEntitiesUrl } from '@app/entity/glossaryTerm/utils';
import UrlButton from '@app/entity/shared/UrlButton';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Deprecation, Domain, EntityType, Owner, ParentNodesResult } from '@types';

export const Preview = ({
    urn,
    name,
    description,
    owners,
    deprecation,
    parentNodes,
    previewType,
    domain,
}: {
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    deprecation?: Deprecation | null;
    parentNodes?: ParentNodesResult | null;
    previewType: PreviewType;
    domain?: Domain | undefined;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            previewType={previewType}
            url={entityRegistry.getEntityUrl(EntityType.GlossaryTerm, urn)}
            name={name || ''}
            urn={urn}
            description={description || ''}
            owners={owners}
            logoComponent={<BookOutlined style={{ fontSize: '20px' }} />}
            type="Glossary Term"
            typeIcon={entityRegistry.getIcon(EntityType.GlossaryTerm, 14, IconStyleType.ACCENT)}
            deprecation={deprecation}
            parentEntities={parentNodes?.nodes}
            domain={domain}
            entityTitleSuffix={
                <UrlButton href={getRelatedEntitiesUrl(entityRegistry, urn)}>View Related Entities</UrlButton>
            }
        />
    );
};
