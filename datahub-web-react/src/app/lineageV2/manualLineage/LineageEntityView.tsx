import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { Entity } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { getPlatformName } from '../../entity/shared/utils';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import { useEntityRegistry } from '../../useEntityRegistry';

const EntityWrapper = styled.div<{ shrinkPadding?: boolean }>`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    padding: ${(props) => (props.shrinkPadding ? '4px 6px' : '12px 20px')};
`;

const PlatformContent = styled.div<{ removeMargin?: boolean }>`
    display: flex;
    align-items: center;
    font-size: 10px;
    color: ${ANTD_GRAY[7]};
    margin-bottom: ${(props) => (props.removeMargin ? '0' : '5px')};
`;

const StyledDivider = styled(Divider)`
    margin: 0 5px;
`;

const PlatformLogo = styled.img`
    height: 14px;
    margin-right: 5px;
`;

export const EntityName = styled.span<{ shrinkSize?: boolean }>`
    font-size: ${(props) => (props.shrinkSize ? '12px' : '14px')};
    font-weight: bold;
`;

interface Props {
    entity: Entity;
    displaySearchResult?: boolean;
}

export default function LineageEntityView({ entity, displaySearchResult }: Props) {
    const entityRegistry = useEntityRegistry();
    const genericProps = entityRegistry.getGenericEntityProperties(entity.type, entity);

    const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;
    const platformName = getPlatformName(genericProps);

    return (
        <EntityWrapper shrinkPadding={displaySearchResult}>
            <PlatformContent removeMargin={displaySearchResult}>
                {platformLogoUrl && (
                    <PlatformLogo src={platformLogoUrl} alt="platform logo" data-testid="platform-logo" />
                )}
                <span>{platformName}</span>
                {platformName && <StyledDivider type="vertical" data-testid="divider" />}
                <span>
                    {capitalizeFirstLetterOnly(genericProps?.subTypes?.typeNames?.[0]) ||
                        entityRegistry.getEntityName(entity.type)}
                </span>
            </PlatformContent>
            <EntityName shrinkSize={displaySearchResult}>
                {entityRegistry.getDisplayName(entity.type, entity)}
            </EntityName>
        </EntityWrapper>
    );
}
