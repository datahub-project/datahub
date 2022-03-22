import * as React from 'react';
import { CodeSandboxOutlined } from '@ant-design/icons';
import { MlModel, EntityType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { MLModelProfile } from './profile/MLModelProfile';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';

/**
 * Definition of the DataHub MlModel entity.
 */
export class MLModelEntity implements Entity<MlModel> {
    type: EntityType = EntityType.Mlmodel;

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

    getPathName = () => 'mlModels';

    getEntityName = () => 'ML Model';

    getCollectionName = () => 'ML Models';

    renderProfile = (urn: string) => <MLModelProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: MlModel) => {
        return <Preview model={data} />;
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlModel;
        return <Preview model={data} />;
    };

    getLineageVizConfig = (entity: MlModel) => {
        return {
            urn: entity.urn,
            name: entity.name,
            type: EntityType.Mlmodel,
            icon: entity.platform?.properties?.logoUrl || undefined,
            platform: entity.platform?.name,
        };
    };

    displayName = (data: MlModel) => {
        return data.name;
    };

    getGenericEntityProperties = (mlModel: MlModel) => {
        return getDataForEntityType({ data: mlModel, entityType: this.type, getOverrideProperties: (data) => data });
    };
}
