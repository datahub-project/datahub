import * as React from 'react';
import { CodeSandboxOutlined } from '@ant-design/icons';
import { MlModelGroup, EntityType, SearchResult, RelationshipDirection } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { MLModelGroupProfile } from './profile/MLModelGroupProfile';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { getChildrenFromRelationships } from '../../lineage/utils/getChildren';

/**
 * Definition of the DataHub MlModelGroup entity.
 */
export class MLModelGroupEntity implements Entity<MlModelGroup> {
    type: EntityType = EntityType.MlmodelGroup;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <CodeSandboxOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <CodeSandboxOutlined style={{ fontSize, color: '#9633b9' }} />;
        }

        return (
            <CodeSandboxOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'mlModelGroup';

    getEntityName = () => 'ML Group';

    getCollectionName = () => 'ML Groups';

    renderProfile = (urn: string) => <MLModelGroupProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: MlModelGroup) => {
        return <Preview group={data} />;
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlModelGroup;
        return <Preview group={data} />;
    };

    getLineageVizConfig = (entity: MlModelGroup) => {
        return {
            urn: entity.urn,
            name: entity.name,
            type: EntityType.MlmodelGroup,
            downstreamChildren: getChildrenFromRelationships({
                // eslint-disable-next-line @typescript-eslint/dot-notation
                incomingRelationships: entity?.['incoming'],
                // eslint-disable-next-line @typescript-eslint/dot-notation
                outgoingRelationships: entity?.['outgoing'],
                direction: RelationshipDirection.Incoming,
            }),
            upstreamChildren: getChildrenFromRelationships({
                // eslint-disable-next-line @typescript-eslint/dot-notation
                incomingRelationships: entity?.['incoming'],
                // eslint-disable-next-line @typescript-eslint/dot-notation
                outgoingRelationships: entity?.['outgoing'],
                direction: RelationshipDirection.Outgoing,
            }),
            icon: entity.platform?.info?.logoUrl || undefined,
            platform: entity.platform?.name,
        };
    };

    displayName = (data: MlModelGroup) => {
        return data.name;
    };

    getGenericEntityProperties = (mlModelGroup: MlModelGroup) => {
        return getDataForEntityType({
            data: mlModelGroup,
            entityType: this.type,
            getOverrideProperties: (data) => data,
        });
    };
}
