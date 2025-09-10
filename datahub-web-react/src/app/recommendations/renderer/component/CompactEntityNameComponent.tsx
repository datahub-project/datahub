import { ArrowRightOutlined } from '@ant-design/icons';
import { TooltipPlacement } from 'antd/es/tooltip';
import React from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { PreviewContextProps } from '@app/entityV2/shared/PreviewContext';
import { EntityPreviewTag } from '@app/recommendations/renderer/component/EntityPreviewTag';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, EntityType, SchemaFieldEntity } from '@types';

const NameWrapper = styled.span<{ addMargin: boolean }>`
    display: inline-flex;
    align-items: center;
    max-width: 100%;
    ${(props) => props.addMargin && 'margin: 2px 0;'}

    .ant-tag {
        padding: 0 4px 0 0;
    }
`;

const StyledArrow = styled(ArrowRightOutlined)`
    color: ${ANTD_GRAY[8]};
    margin: 0 4px;
`;

const FullWidthContainer = styled.span`
    max-width: 100%;
`;

const StyledPlatformIcon = styled(PlatformIcon)`
    padding: 0;
`;

type CompactEntityNameProps = {
    entity: Entity;
    showFullTooltip?: boolean;
    showArrow?: boolean;
    placement?: TooltipPlacement;
    onClick?: () => void;
    linkUrlParams?: Record<string, string | boolean>;
    isProposed?: boolean;
    showMargin?: boolean;
    previewContext?: PreviewContextProps;
};

export const CompactEntityNameComponent = ({
    entity,
    showFullTooltip = true,
    showArrow = false,
    placement,
    onClick,
    linkUrlParams,
    isProposed,
    showMargin = true,
    previewContext,
}: CompactEntityNameProps) => {
    const entityRegistry = useEntityRegistry();

    if (!entity) return null;

    let processedEntity = entity;
    let columnName;

    if (entity.type === EntityType.SchemaField) {
        const { parent, fieldPath } = entity as SchemaFieldEntity;
        processedEntity = parent ?? { urn: entity.urn, type: EntityType.Dataset };
        columnName = fieldPath;
    }

    const genericProps = entityRegistry.getGenericEntityProperties(processedEntity.type, processedEntity);
    const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;
    const displayName = entityRegistry.getDisplayName(processedEntity.type, processedEntity);
    const fallbackIcon = (
        <StyledPlatformIcon platform={genericProps?.platform} size={12} alt={genericProps?.platform?.name} />
    );
    const url = entityRegistry.getEntityUrl(processedEntity.type, processedEntity.urn, linkUrlParams);

    return (
        <NameWrapper addMargin={showArrow}>
            <HoverEntityTooltip
                entity={processedEntity}
                canOpen={showFullTooltip}
                placement={placement}
                showArrow={false}
                previewContext={previewContext}
            >
                <FullWidthContainer>
                    <EntityPreviewTag
                        showNameTooltip={!showFullTooltip}
                        displayName={displayName}
                        url={url}
                        platformLogoUrl={platformLogoUrl || undefined}
                        platformLogoUrls={genericProps?.siblingPlatforms?.map(
                            (platform) => platform.properties?.logoUrl,
                        )}
                        logoComponent={fallbackIcon}
                        onClick={onClick}
                        columnName={columnName}
                        dataTestId={`compact-entity-link-${processedEntity.urn}`}
                        isProposed={isProposed}
                        showMargin={showMargin}
                    />
                </FullWidthContainer>
            </HoverEntityTooltip>
            {showArrow && <StyledArrow />}
        </NameWrapper>
    );
};
