import { GenericEntityProperties } from '@app/entity/shared/types';
import React from 'react';
import {
    Container,
    EntityType,
    Owner,
    SearchInsight,
    SubTypes,
    Domain,
    ParentContainersResult,
    GlobalTags,
    Deprecation,
    GlossaryTerms,
    DataProduct,
    EntityPath,
    BrowsePathV2,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import ContainerIcon from '../../shared/containers/profile/header/PlatformContent/ContainerIcon';
import EntityCount from '../../shared/containers/profile/header/EntityCount';
import { EntityMenuItems } from '../../shared/EntityDropdown/EntityMenuActions';

export const Preview = ({
    urn,
    data,
    name,
    platformName,
    platformLogo,
    platformInstanceId,
    description,
    owners,
    tags,
    glossaryTerms,
    insights,
    subTypes,
    logoComponent,
    container,
    domain,
    dataProduct,
    parentContainers,
    externalUrl,
    deprecation,
    degree,
    paths,
    entityCount,
    isOutputPort,
    headerDropdownItems,
    browsePaths,
}: {
    urn: string;
    data: GenericEntityProperties | null;
    name: string;
    platformName?: string;
    platformLogo?: string | null;
    platformInstanceId?: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    tags?: GlobalTags | null;
    glossaryTerms?: GlossaryTerms | null;
    insights?: Array<SearchInsight> | null;
    subTypes?: SubTypes | null;
    logoComponent?: JSX.Element;
    container?: Container | null;
    domain?: Domain | null;
    dataProduct?: DataProduct | null;
    deprecation?: Deprecation | null;
    parentContainers?: ParentContainersResult | null;
    externalUrl?: string | null;
    degree?: number;
    paths?: EntityPath[];
    entityCount?: number;
    isOutputPort?: boolean;
    headerDropdownItems?: Set<EntityMenuItems>;
    browsePaths?: BrowsePathV2;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Container, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            platform={platformName}
            platformInstanceId={platformInstanceId}
            description={description || ''}
            entityType={EntityType.Container}
            type={subTypes?.typeNames?.[0]}
            owners={owners}
            deprecation={deprecation}
            insights={insights}
            logoUrl={platformLogo || undefined}
            logoComponent={logoComponent}
            container={container || undefined}
            typeIcon={<ContainerIcon container={container} />}
            domain={domain || undefined}
            dataProduct={dataProduct}
            parentEntities={parentContainers?.containers}
            tags={tags || undefined}
            glossaryTerms={glossaryTerms || undefined}
            externalUrl={externalUrl}
            degree={degree}
            paths={paths}
            subHeader={<EntityCount displayAssetsText entityCount={entityCount} />}
            isOutputPort={isOutputPort}
            headerDropdownItems={headerDropdownItems}
            browsePaths={browsePaths}
        />
    );
};
