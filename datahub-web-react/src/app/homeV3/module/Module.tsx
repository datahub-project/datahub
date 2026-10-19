import React, { memo, useMemo } from 'react';

import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';
import AiContextModule from '@app/entityV2/summary/modules/aiContext/AiContextModule';
import AssetsModule from '@app/entityV2/summary/modules/assets/AssetsModule';
import ChildHierarchyModule from '@app/entityV2/summary/modules/childHierarchy/ChildHierarchyModule';
import DataProductsModule from '@app/entityV2/summary/modules/dataProducts/DataProductsModule';
import OutputPortsModule from '@app/entityV2/summary/modules/outputPorts/OutputPortsModule';
import RelatedMetricsModule from '@app/entityV2/summary/modules/relatedMetrics/RelatedMetricsModule';
import RelatedTermsModule from '@app/entityV2/summary/modules/relatedTerms/RelatedTermsModule';
import SemanticModelDatasetsModule from '@app/entityV2/summary/modules/semanticModelDatasets/SemanticModelDatasetsModule';
import SemanticModelDimensionsModule from '@app/entityV2/summary/modules/semanticModelDimensions/SemanticModelDimensionsModule';
import SemanticModelMetricsModule from '@app/entityV2/summary/modules/semanticModelMetrics/SemanticModelMetricsModule';
import SemanticModelRelationshipsModule from '@app/entityV2/summary/modules/semanticModelRelationships/SemanticModelRelationshipsModule';
import SubDataProductsModule from '@app/entityV2/summary/modules/subDataProducts/SubDataProductsModule';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import ModuleErrorBoundary from '@app/homeV3/module/components/ModuleErrorBoundary';
import { ModuleProvider } from '@app/homeV3/module/context/ModuleContext';
import { ModuleProps } from '@app/homeV3/module/types';
import SampleLargeModule from '@app/homeV3/modules/SampleLargeModule';
import YourAssetsModule from '@app/homeV3/modules/YourAssetsModule';
import AssetCollectionModule from '@app/homeV3/modules/assetCollection/AssetCollectionModule';
import DocumentationModule from '@app/homeV3/modules/documentation/DocumentationModule';
import TopDomainsModule from '@app/homeV3/modules/domains/TopDomainsModule';
import HierarchyViewModule from '@app/homeV3/modules/hierarchyViewModule/HierarchyViewModule';
import LinkModule from '@app/homeV3/modules/link/LinkModule';
import PlatformsModule from '@app/homeV3/modules/platforms/PlatformsModule';

import { DataHubPageModuleType } from '@types';

// Home and asset summary render every module type from this file, so these load on demand:
// LineageModule pulls React Flow, MetricSqlModule the syntax highlighter, and ColumnsModule the
// schema table. The loading frame keeps the module's height, and render errors go to
// ModuleErrorBoundary so its retry works.
const largeModuleChunkOptions = {
    fallback: (props: ModuleProps) => <LargeModule {...props} loading />,
    propagateRenderErrors: true,
};
const LineageModule = lazyProfileComponent(
    'LineageModule',
    () => import('@app/entityV2/summary/modules/lineage/LineageModule'),
    largeModuleChunkOptions,
);
const ColumnsModule = lazyProfileComponent(
    'ColumnsModule',
    () => import('@app/entityV2/summary/modules/schemaTable/ColumnsModule'),
    largeModuleChunkOptions,
);
const MetricSqlModule = lazyProfileComponent(
    'MetricSqlModule',
    () => import('@app/entityV2/summary/modules/sql/MetricSqlModule'),
    largeModuleChunkOptions,
);

function Module(props: ModuleProps) {
    const { module } = props;

    // Memoize component selection to prevent re-evaluation on every render
    const Component = useMemo(() => {
        if (module.properties.type === DataHubPageModuleType.OwnedAssets) return YourAssetsModule;
        if (module.properties.type === DataHubPageModuleType.Domains) return TopDomainsModule;
        if (module.properties.type === DataHubPageModuleType.AssetCollection) return AssetCollectionModule;
        if (module.properties.type === DataHubPageModuleType.Link) return LinkModule;
        if (module.properties.type === DataHubPageModuleType.RichText) return DocumentationModule;
        if (module.properties.type === DataHubPageModuleType.Hierarchy) return HierarchyViewModule;
        if (module.properties.type === DataHubPageModuleType.Assets) return AssetsModule;
        if (module.properties.type === DataHubPageModuleType.OutputPorts) return OutputPortsModule;
        if (module.properties.type === DataHubPageModuleType.ChildHierarchy) return ChildHierarchyModule;
        if (module.properties.type === DataHubPageModuleType.DataProducts) return DataProductsModule;
        if (module.properties.type === DataHubPageModuleType.SubDataProducts) return SubDataProductsModule;
        if (module.properties.type === DataHubPageModuleType.RelatedTerms) return RelatedTermsModule;
        if (module.properties.type === DataHubPageModuleType.Platforms) return PlatformsModule;
        if (module.properties.type === DataHubPageModuleType.Lineage) return LineageModule;
        if (module.properties.type === DataHubPageModuleType.Columns) return ColumnsModule;
        if (module.properties.type === DataHubPageModuleType.AiContext) return AiContextModule;
        if (module.properties.type === DataHubPageModuleType.SemanticModelDatasets) return SemanticModelDatasetsModule;
        if (module.properties.type === DataHubPageModuleType.SemanticModelMetrics) return SemanticModelMetricsModule;
        if (module.properties.type === DataHubPageModuleType.SemanticModelRelationships)
            return SemanticModelRelationshipsModule;
        if (module.properties.type === DataHubPageModuleType.SemanticModelDimensions)
            return SemanticModelDimensionsModule;
        if (module.properties.type === DataHubPageModuleType.MetricSql) return MetricSqlModule;
        if (module.properties.type === DataHubPageModuleType.RelatedMetrics) return RelatedMetricsModule;

        // TODO: remove the sample large module once we have other modules to fill this out
        console.error(`Issue finding module with type ${module.properties.type}`);
        return SampleLargeModule;
    }, [module.properties.type]);

    return (
        <ModuleErrorBoundary {...props}>
            <ModuleProvider {...props}>
                <Component {...props} />
            </ModuleProvider>
        </ModuleErrorBoundary>
    );
}

// Export memoized component to prevent unnecessary re-renders
const MemoizedModule = memo(Module);
MemoizedModule.displayName = 'Module';

export default MemoizedModule;
