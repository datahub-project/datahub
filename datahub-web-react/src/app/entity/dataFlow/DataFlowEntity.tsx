import * as React from 'react';
import { ShareAltOutlined } from '@ant-design/icons';
import { DataFlow, EntityType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { DataFlowProfile } from './profile/DataFlowProfile';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getLogoFromPlatform } from '../../shared/getLogoFromPlatform';

/**
 * Definition of the DataHub DataFlow entity.
 */
export class DataFlowEntity implements Entity<DataFlow> {
    type: EntityType = EntityType.DataFlow;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <ShareAltOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <ShareAltOutlined style={{ fontSize, color: '#d6246c' }} />;
        }

        return (
            <ShareAltOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'pipelines';

    getCollectionName = () => 'Pipelines';

    renderProfile = (urn: string) => <DataFlowProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: DataFlow) => {
        const platformName = data.orchestrator.charAt(0).toUpperCase() + data.orchestrator.slice(1);
        return (
            <Preview
                urn={data.urn}
                name={data.info?.name || ''}
                description={data.editableProperties?.description || data.info?.description}
                platformName={platformName}
                platformLogo={getLogoFromPlatform(data.orchestrator)}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataFlow;
        const platformName = data.orchestrator.charAt(0).toUpperCase() + data.orchestrator.slice(1);
        return (
            <Preview
                urn={data.urn}
                name={data.info?.name || ''}
                description={data.editableProperties?.description || data.info?.description || ''}
                platformName={platformName}
                platformLogo={getLogoFromPlatform(data.orchestrator)}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
            />
        );
    };

    displayName = (data: DataFlow) => {
        return data.info?.name || data.urn;
    };
}
