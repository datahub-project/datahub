import EntityRegistry from './entityV2/EntityRegistry';
import { DashboardEntity } from './entityV2/dashboard/DashboardEntity';
import { ChartEntity } from './entityV2/chart/ChartEntity';
import { UserEntity } from './entityV2/user/User';
import { GroupEntity } from './entityV2/group/Group';
import { DatasetEntity } from './entityV2/dataset/DatasetEntity';
import { DataFlowEntity } from './entityV2/dataFlow/DataFlowEntity';
import { DataJobEntity } from './entityV2/dataJob/DataJobEntity';
import { TagEntity } from './entityV2/tag/Tag';
import { GlossaryTermEntity } from './entityV2/glossaryTerm/GlossaryTermEntity';
import { MLFeatureEntity } from './entityV2/mlFeature/MLFeatureEntity';
import { MLPrimaryKeyEntity } from './entityV2/mlPrimaryKey/MLPrimaryKeyEntity';
import { MLFeatureTableEntity } from './entityV2/mlFeatureTable/MLFeatureTableEntity';
import { MLModelEntity } from './entityV2/mlModel/MLModelEntity';
import { MLModelGroupEntity } from './entityV2/mlModelGroup/MLModelGroupEntity';
import { DomainEntity } from './entityV2/domain/DomainEntity';
import { ContainerEntity } from './entityV2/container/ContainerEntity';
import GlossaryNodeEntity from './entityV2/glossaryNode/GlossaryNodeEntity';
import { DataPlatformEntity } from './entityV2/dataPlatform/DataPlatformEntity';
import { DataProductEntity } from './entityV2/dataProduct/DataProductEntity';
import { DataPlatformInstanceEntity } from './entityV2/dataPlatformInstance/DataPlatformInstanceEntity';
import { RoleEntity } from './entityV2/Access/RoleEntity';
import { QueryEntity } from './entityV2/query/QueryEntity';
import { SchemaFieldEntity } from './entityV2/schemaField/SchemaFieldEntity';
import { StructuredPropertyEntity } from './entityV2/structuredProperty/StructuredPropertyEntity';
import { DataProcessInstanceEntity } from './entityV2/dataProcessInstance/DataProcessInstanceEntity';
import { BusinessAttributeEntity } from './entityV2/businessAttribute/BusinessAttributeEntity';

export default function buildEntityRegistryV2() {
    const registry = new EntityRegistry();
    registry.register(new DatasetEntity());
    registry.register(new DashboardEntity());
    registry.register(new ChartEntity());
    registry.register(new UserEntity());
    registry.register(new GroupEntity());
    registry.register(new TagEntity());
    registry.register(new DataFlowEntity());
    registry.register(new DataJobEntity());
    registry.register(new GlossaryTermEntity());
    registry.register(new MLFeatureEntity());
    registry.register(new MLPrimaryKeyEntity());
    registry.register(new MLFeatureTableEntity());
    registry.register(new MLModelEntity());
    registry.register(new MLModelGroupEntity());
    registry.register(new DomainEntity());
    registry.register(new ContainerEntity());
    registry.register(new GlossaryNodeEntity());
    registry.register(new RoleEntity());
    registry.register(new DataPlatformEntity());
    registry.register(new DataProductEntity());
    registry.register(new DataPlatformInstanceEntity());
    registry.register(new QueryEntity());
    registry.register(new SchemaFieldEntity());
    registry.register(new StructuredPropertyEntity());
    registry.register(new DataProcessInstanceEntity());
    registry.register(new BusinessAttributeEntity());
    return registry;
}