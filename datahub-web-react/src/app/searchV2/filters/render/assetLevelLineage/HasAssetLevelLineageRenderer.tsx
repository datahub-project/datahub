import { TreeStructure } from '@phosphor-icons/react';
import React from 'react';

import { FilterRenderer } from '@app/searchV2/filters/render/FilterRenderer';
import { HasAssetLevelLineageFilter } from '@app/searchV2/filters/render/assetLevelLineage/HasAssetLevelLineageFilter';
import { FilterRenderProps } from '@app/searchV2/filters/render/types';
import { FIELD_TO_LABEL, HAS_ASSET_LEVEL_LINEAGE_FILTER_NAME } from '@app/searchV2/utils/constants';

export class HasAssetLevelLineageRenderer implements FilterRenderer {
    field = HAS_ASSET_LEVEL_LINEAGE_FILTER_NAME;

    hasValueLabel = FIELD_TO_LABEL[HAS_ASSET_LEVEL_LINEAGE_FILTER_NAME];

    render = (props: FilterRenderProps) => <HasAssetLevelLineageFilter {...props} icon={this.icon()} />;

    icon = () => <TreeStructure />;

    valueLabel = (value: string) => {
        if (value === 'true') {
            return <>{this.hasValueLabel}</>;
        }
        return <>Has No Asset-Level Lineage</>;
    };
}
