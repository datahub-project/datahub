import React from 'react';
import styled from 'styled-components';
import { EntityType, MlModel } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { capitalizeFirstLetter } from '../../../shared/textUtil';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';

const LogoContainer = styled.div`
    padding-right: 8px;
`;

export const Preview = ({ model }: { model: MlModel }): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const capitalPlatformName = capitalizeFirstLetter(model?.platform?.name || '');

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Mlmodel, model.urn)}
            name={model.name || ''}
            description={model.description || ''}
            platformInstanceId={model.dataPlatformInstance?.instanceId}
            type={entityRegistry.getEntityName(EntityType.Mlmodel)}
            logoComponent={
                <LogoContainer>{entityRegistry.getIcon(EntityType.Mlmodel, 20, IconStyleType.HIGHLIGHT)}</LogoContainer>
            }
            platform={capitalPlatformName}
            qualifier={model.origin}
            tags={model.globalTags || undefined}
            owners={model?.ownership?.owners}
        />
    );
};
