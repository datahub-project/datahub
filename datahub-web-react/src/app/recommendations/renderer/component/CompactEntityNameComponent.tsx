import { ArrowRightOutlined } from '@ant-design/icons';
import { TooltipPlacement } from 'antd/es/tooltip';
import React from 'react';
import styled from 'styled-components/macro';
import { Entity, EntityType, SchemaFieldEntity } from '../../../../types.generated';
import { IconStyleType } from '../../../entity/Entity';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityPreviewTag } from './EntityPreviewTag';
import { HoverEntityTooltip } from './HoverEntityTooltip';

const NameWrapper = styled.span<{ addMargin: boolean }>`
    display: inline-flex;
    align-items: center;
    max-width: 100%;
    ${(props) => props.addMargin && 'margin: 2px 0;'}
`;

const StyledArrow = styled(ArrowRightOutlined)`
    color: ${ANTD_GRAY[8]};
    margin: 0 4px;
`;

type CompactEntityNameProps = {
    entity: Entity;
    showFullTooltip?: boolean;
    showArrow?: boolean;
    placement?: TooltipPlacement;
    onClick?: () => void;
    linkUrlParams?: Record<string, string | boolean>;
};

export const CompactEntityNameComponent = ({
    entity,
    showFullTooltip = true,
    showArrow = false,
    placement,
    onClick,
    linkUrlParams,
}: CompactEntityNameProps) => {
    const entityRegistry = useEntityRegistry();

    if (!entity) return null;

    let processedEntity = entity;
    let columnName;

    if (entity.type === EntityType.SchemaField) {
        const { parent, fieldPath } = entity as SchemaFieldEntity;
        processedEntity = parent;
        columnName = fieldPath;
    }

    const genericProps = entityRegistry.getGenericEntityProperties(processedEntity.type, processedEntity);
    const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;
    const displayName = entityRegistry.getDisplayName(processedEntity.type, processedEntity);
    const fallbackIcon = entityRegistry.getIcon(processedEntity.type, 12, IconStyleType.ACCENT);
    const url = entityRegistry.getEntityUrl(processedEntity.type, processedEntity.urn, linkUrlParams);

    return (
        <NameWrapper addMargin={showArrow}>
            <HoverEntityTooltip
                entity={processedEntity}
                canOpen={showFullTooltip}
                placement={placement}
                showArrow={false}
            >
                <span>
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
                    />
                </span>
            </HoverEntityTooltip>
            {showArrow && <StyledArrow />}
        </NameWrapper>
    );
};
