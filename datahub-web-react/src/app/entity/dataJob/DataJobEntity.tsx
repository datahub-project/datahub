import * as React from 'react';
import { ConsoleSqlOutlined } from '@ant-design/icons';
import { DataJob, EntityType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { DataJobProfile } from './profile/DataJobProfile';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import getChildren from '../../lineage/utils/getChildren';
import { Direction } from '../../lineage/types';
import { getLogoFromPlatform } from '../../shared/getLogoFromPlatform';

/**
 * Definition of the DataHub DataJob entity.
 */
export class DataJobEntity implements Entity<DataJob> {
    type: EntityType = EntityType.DataJob;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <ConsoleSqlOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <ConsoleSqlOutlined style={{ fontSize, color: '#B37FEB' }} />;
        }

        return (
            <ConsoleSqlOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'tasks';

    getCollectionName = () => 'Task';

    renderProfile = (urn: string) => <DataJobProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: DataJob) => {
        const platformName = data.dataFlow
            ? data.dataFlow?.orchestrator.charAt(0).toUpperCase() + data.dataFlow?.orchestrator.slice(1)
            : '';
        return (
            <Preview
                urn={data.urn}
                name={data.info?.name || ''}
                description={data.editableProperties?.description || data.info?.description}
                platformName={platformName}
                platformLogo={getLogoFromPlatform(data.dataFlow?.orchestrator || '')}
                owners={data.ownership?.owners}
                globalTags={data.globalTags || null}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataJob;
        const platformName = data.dataFlow
            ? data.dataFlow?.orchestrator.charAt(0).toUpperCase() + data.dataFlow?.orchestrator.slice(1)
            : '';
        return (
            <Preview
                urn={data.urn}
                name={data.info?.name || ''}
                description={data.editableProperties?.description || data.info?.description}
                platformName={platformName}
                platformLogo={getLogoFromPlatform(data.dataFlow?.orchestrator || '')}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
            />
        );
    };

    getLineageVizConfig = (entity: DataJob) => {
        return {
            urn: entity.urn,
            name: entity.info?.name || '',
            type: EntityType.DataJob,
            upstreamChildren: getChildren({ entity, type: EntityType.DataJob }, Direction.Upstream).map(
                (child) => child.entity.urn,
            ),
            downstreamChildren: getChildren({ entity, type: EntityType.DataJob }, Direction.Downstream).map(
                (child) => child.entity.urn,
            ),
            icon: getLogoFromPlatform(entity.dataFlow?.orchestrator || ''),
            platform: entity.dataFlow?.orchestrator || '',
        };
    };
}
