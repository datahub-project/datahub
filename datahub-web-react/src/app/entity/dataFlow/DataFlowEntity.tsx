import * as React from 'react';
import { DatabaseFilled, DatabaseOutlined } from '@ant-design/icons';
import { DataFlow, EntityType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { DataFlowProfile } from './profile/DataFlowProfile';
import { Entity, IconStyleType, PreviewType } from '../Entity';

/**
 * Definition of the DataHub DataFlow entity.
 */
export class DataFlowEntity implements Entity<DataFlow> {
    type: EntityType = EntityType.DataFlow;

    // TODO: add flow specific icons
    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DatabaseOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <DatabaseFilled style={{ fontSize, color: '#B37FEB' }} />;
        }

        return (
            <DatabaseFilled
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

    getPathName = () => 'dataflow';

    getCollectionName = () => 'DataFlows';

    renderProfile = (urn: string) => <DataFlowProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: DataFlow) => {
        return (
            <Preview
                urn={data.urn}
                name={data.info?.name || ''}
                description={data.info?.description || ''}
                platformName={data.info?.project || ''}
                owners={data.ownership?.owners}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataFlow;
        return (
            <Preview
                urn={data.urn}
                name={data.info?.name || ''}
                description={data.info?.description || ''}
                platformName={data.info?.project || ''}
                owners={data.ownership?.owners}
            />
        );
    };
}
