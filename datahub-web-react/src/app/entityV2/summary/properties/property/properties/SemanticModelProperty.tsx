import { Avatar } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { EntityType, Metric, SemanticModel } from '@types';

type MetricWithSemanticModel = Metric & {
    semanticModel?: SemanticModel | null;
};

export default function SemanticModelProperty(props: PropertyComponentProps) {
    const entityRegistry = useEntityRegistryV2();
    const { entityData, loading } = useEntityContext();
    const semanticModel = (entityData as MetricWithSemanticModel)?.semanticModel;

    const renderSemanticModel = (sm: SemanticModel) => {
        const displayName = entityRegistry.getDisplayName(EntityType.SemanticModel, sm);
        return (
            <HoverEntityTooltip entity={sm} showArrow={false}>
                <Link to={entityRegistry.getEntityUrl(EntityType.SemanticModel, sm.urn)}>
                    <Avatar name={displayName} size="sm" showInPill />
                </Link>
            </HoverEntityTooltip>
        );
    };

    return (
        <BaseProperty
            {...props}
            values={semanticModel ? [semanticModel] : []}
            renderValue={renderSemanticModel}
            loading={loading}
        />
    );
}
