import { FileText } from '@phosphor-icons/react';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { DocumentProfile } from '@app/entityV2/document/DocumentProfile';
import { Preview } from '@app/entityV2/document/preview/Preview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import EmbeddedProfile from '@app/entityV2/shared/embed/EmbeddedProfile';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';

import { useGetDocumentQuery } from '@graphql/document.generated';
import { Document, EntityType, SearchResult } from '@types';

const headerDropdownItems = new Set([
    EntityMenuItems.COPY_URL,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.SHARE,
    EntityMenuItems.DELETE,
    EntityMenuItems.ANNOUNCE,
]);

/**
 * Definition of the DataHub Document entity.
 */
export class DocumentEntity implements Entity<Document> {
    type: EntityType = EntityType.Document;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.SVG) {
            return (
                <path
                    d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z M13 2v7h7 M11 14H8 M16 14h-2 M16 18H8"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    fill="none"
                />
            );
        }

        return (
            <FileText
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

    getAutoCompleteFieldName = () => 'title';

    getGraphName = () => 'document';

    getPathName = () => 'document';

    getEntityName = () => 'Document';

    getCollectionName = () => 'Documents';

    useEntityQuery = useGetDocumentQuery;

    renderProfile = (urn: string) => <DocumentProfile urn={urn} />;

    renderPreview = (previewType: PreviewType, data: Document) => {
        const genericProperties = this.getGenericEntityProperties(data);
        const platform = genericProperties?.platform?.urn !== 'urn:li:dataPlatform:datahub' ? data.platform : undefined;
        return (
            <Preview
                document={data}
                urn={data.urn}
                data={genericProperties}
                name={this.displayName(data)}
                description={data.info?.contents?.text}
                platformName={
                    platform?.properties?.displayName || (platform?.name && capitalizeFirstLetterOnly(platform.name))
                }
                platformLogo={platform?.properties?.logoUrl}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                owners={data.ownership?.owners}
                logoComponent={this.icon(12, IconStyleType.ACCENT)}
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Document;
        const genericProperties = this.getGenericEntityProperties(data);
        const platform = genericProperties?.platform?.urn !== 'urn:li:dataPlatform:datahub' ? data.platform : undefined;
        return (
            <Preview
                document={data}
                urn={data.urn}
                data={genericProperties}
                name={this.displayName(data)}
                description={data.info?.contents?.text}
                platformName={
                    platform?.properties?.displayName || (platform?.name && capitalizeFirstLetterOnly(platform.name))
                }
                platformLogo={platform?.properties?.logoUrl}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                owners={data.ownership?.owners}
                logoComponent={this.icon(12, IconStyleType.ACCENT)}
                headerDropdownItems={headerDropdownItems}
                previewType={PreviewType.SEARCH}
            />
        );
    };

    displayName = (data: Document) => {
        return data?.info?.title || data?.urn;
    };

    getOverridePropertiesFromEntity = (data: Document) => {
        const externalUrl = data.info?.source?.externalUrl;
        return {
            name: data.info?.title,
            externalUrl,
            properties: {
                name: data.info?.title,
                externalUrl,
            },
        };
    };

    getGenericEntityProperties = (data: Document) => {
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
            EntityCapabilityType.DATA_PRODUCTS,
        ]);
    };

    renderEmbeddedProfile = (urn: string) => (
        <EmbeddedProfile
            urn={urn}
            entityType={EntityType.Document}
            useEntityQuery={useGetDocumentQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
        />
    );
}
