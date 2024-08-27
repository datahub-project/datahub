import React from 'react';
import { BookOutlined } from '@ant-design/icons';
import { useTranslation } from 'react-i18next';
import { Deprecation, Domain, EntityType, Owner, ParentNodesResult } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType, PreviewType } from '../../Entity';
import UrlButton from '../../shared/UrlButton';
import { getRelatedEntitiesUrl } from '../utils';

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
    const { t } = useTranslation();
    return (
        <DefaultPreviewCard
            previewType={previewType}
            url={entityRegistry.getEntityUrl(EntityType.GlossaryTerm, urn)}
            name={name || ''}
            urn={urn}
            description={description || ''}
            owners={owners}
            logoComponent={<BookOutlined style={{ fontSize: '20px' }} />}
            type={t('common.glossaryTerms')}
            typeIcon={entityRegistry.getIcon(EntityType.GlossaryTerm, 14, IconStyleType.ACCENT)}
            deprecation={deprecation}
            parentEntities={parentNodes?.nodes}
            domain={domain}
            entityTitleSuffix={
                <UrlButton href={getRelatedEntitiesUrl(entityRegistry, urn)}>
                    {t('filter.view.viewRekatedEntities')}
                </UrlButton>
            }
        />
    );
};
