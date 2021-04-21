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

        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M342 88H120c-17.7 0-32 14.3-32 32v224c0 8.8 7.2 16 16 16h48c8.8 0 16-7.2 16-16V168h174c8.8 0 16-7.2 16-16v-48c0-8.8-7.2-16-16-16zm578 576h-48c-8.8 0-16 7.2-16 16v176H682c-8.8 0-16 7.2-16 16v48c0 8.8 7.2 16 16 16h222c17.7 0 32-14.3 32-32V680c0-8.8-7.2-16-16-16zM342 856H168V680c0-8.8-7.2-16-16-16h-48c-8.8 0-16 7.2-16 16v224c0 17.7 14.3 32 32 32h222c8.8 0 16-7.2 16-16v-48c0-8.8-7.2-16-16-16zM904 88H682c-8.8 0-16 7.2-16 16v48c0 8.8 7.2 16 16 16h174v176c0 8.8 7.2 16 16 16h48c8.8 0 16-7.2 16-16V120c0-17.7-14.3-32-32-32z" />
            );
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

    isSearchEnabled = () => false;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

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
