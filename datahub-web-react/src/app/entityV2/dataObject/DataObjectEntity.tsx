import { File } from '@phosphor-icons/react/dist/csr/File';
import i18next from 'i18next';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { DataObjectProfile } from '@app/entityV2/dataObject/DataObjectProfile';
import { Preview } from '@app/entityV2/dataObject/preview/Preview';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import EmbeddedProfile from '@app/entityV2/shared/embed/EmbeddedProfile';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';

import { useGetDataObjectQuery } from '@graphql/dataObject.generated';
import { DataObject, EntityType, SearchResult } from '@types';

export class DataObjectEntity implements Entity<DataObject> {
    type: EntityType = EntityType.DataObject;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.SVG) {
            return (
                <path
                    d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z M13 2v7h7"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    fill="none"
                />
            );
        }

        return (
            <File
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'dataObject';

    getPathName = () => 'dataObject';

    getEntityName = () => i18next.t('entity.types:dataObject.name');

    getCollectionName = () => i18next.t('entity.types:dataObject.namePlural');

    useEntityQuery = useGetDataObjectQuery;

    renderProfile = (urn: string) => <DataObjectProfile urn={urn} />;

    renderPreview = (previewType: PreviewType, data: DataObject) => {
        const genericProperties = this.getGenericEntityProperties(data);
        const platform = genericProperties?.platform?.urn !== 'urn:li:dataPlatform:datahub' ? data.platform : undefined;
        return (
            <Preview
                dataObject={data}
                urn={data.urn}
                data={genericProperties}
                name={this.displayName(data)}
                subTypes={data.subTypes}
                description={data.properties?.description}
                platformName={
                    platform?.properties?.displayName || (platform?.name && capitalizeFirstLetterOnly(platform.name))
                }
                platformLogo={platform?.properties?.logoUrl}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                owners={data.ownership?.owners}
                logoComponent={this.icon(12, IconStyleType.ACCENT)}
                previewType={previewType}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataObject;
        const genericProperties = this.getGenericEntityProperties(data);
        const platform = genericProperties?.platform?.urn !== 'urn:li:dataPlatform:datahub' ? data.platform : undefined;
        return (
            <Preview
                dataObject={data}
                urn={data.urn}
                data={genericProperties}
                name={this.displayName(data)}
                subTypes={data.subTypes}
                description={data.properties?.description}
                platformName={
                    platform?.properties?.displayName || (platform?.name && capitalizeFirstLetterOnly(platform.name))
                }
                platformLogo={platform?.properties?.logoUrl}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                owners={data.ownership?.owners}
                logoComponent={this.icon(12, IconStyleType.ACCENT)}
                previewType={PreviewType.SEARCH}
            />
        );
    };

    displayName = (data: DataObject) => {
        return data?.properties?.name || data?.name || data?.urn;
    };

    getOverridePropertiesFromEntity = (data: DataObject) => {
        const externalUrl = data.properties?.externalUrl;
        return {
            name: data.properties?.name || data.name,
            externalUrl,
            properties: {
                name: data.properties?.name || data.name,
                externalUrl,
            },
        };
    };

    getGenericEntityProperties = (data: DataObject) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };

    supportedCapabilities = () => {
        return new Set([
            EntityCapabilityType.OWNERS,
            EntityCapabilityType.TAGS,
            EntityCapabilityType.GLOSSARY_TERMS,
            EntityCapabilityType.DOMAINS,
            // dataObjects are storage assets (like datasets/containers) and can be grouped into
            // Data Products and Applications. The "Add Assets" pickers are capability-derived
            // (entityRegistry.getTypesWithSupportedCapabilities), so this declaration is the single
            // source of truth — there is no parallel hardcoded allowlist that could drift.
            EntityCapabilityType.DATA_PRODUCTS,
            EntityCapabilityType.APPLICATIONS,
        ]);
    };

    renderEmbeddedProfile = (urn: string) => (
        <EmbeddedProfile
            urn={urn}
            entityType={EntityType.DataObject}
            useEntityQuery={useGetDataObjectQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
        />
    );
}
