import * as React from 'react';
import { DoubleRightOutlined } from '@ant-design/icons';
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
            return <DoubleRightOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <DoubleRightOutlined style={{ fontSize, color: '#B37FEB' }} />;
        }

        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M533.2 492.3L277.9 166.1c-3-3.9-7.7-6.1-12.6-6.1H188c-6.7 0-10.4 7.7-6.3 12.9L447.1 512 181.7 851.1A7.98 7.98 0 00188 864h77.3c4.9 0 9.6-2.3 12.6-6.1l255.3-326.1c9.1-11.7 9.1-27.9 0-39.5zm304 0L581.9 166.1c-3-3.9-7.7-6.1-12.6-6.1H492c-6.7 0-10.4 7.7-6.3 12.9L751.1 512 485.7 851.1A7.98 7.98 0 00492 864h77.3c4.9 0 9.6-2.3 12.6-6.1l255.3-326.1c9.1-11.7 9.1-27.9 0-39.5z" />
            );
        }

        return (
            <DoubleRightOutlined
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

    getCollectionName = () => 'Tasks';

    renderProfile = (urn: string) => <DataJobProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: DataJob) => {
        const platformName = data.dataFlow?.orchestrator.charAt(0).toUpperCase() + data.dataFlow?.orchestrator.slice(1);
        return (
            <Preview
                urn={data.urn}
                name={data.info?.name || ''}
                description={data.info?.description}
                platformName={platformName}
                platformLogo={getLogoFromPlatform(data.dataFlow?.orchestrator)}
                owners={data.ownership?.owners}
                globalTags={data.globalTags || null}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataJob;
        const platformName = data.dataFlow?.orchestrator.charAt(0).toUpperCase() + data.dataFlow?.orchestrator.slice(1);
        return (
            <Preview
                urn={data.urn}
                name={data.info?.name || ''}
                description={data.info?.description}
                platformName={platformName}
                platformLogo={getLogoFromPlatform(data.dataFlow?.orchestrator)}
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
            icon: getLogoFromPlatform(entity.dataFlow.orchestrator),
            platform: entity.dataFlow.orchestrator,
        };
    };
}
