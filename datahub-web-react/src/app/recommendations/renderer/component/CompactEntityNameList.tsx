import { ArrowRightOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components/macro';
import { useHistory } from 'react-router';
import { Entity, EntityType, SchemaFieldEntity } from '../../../../types.generated';
import { IconStyleType } from '../../../entity/Entity';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityPreviewTag } from './EntityPreviewTag';
import { HoverEntityTooltip } from './HoverEntityTooltip';
import { ANTD_GRAY } from '../../../entity/shared/constants';

const NameWrapper = styled.span<{ addMargin }>`
    display: inline-flex;
    align-items: center;
    ${(props) => props.addMargin && 'margin: 2px 0;'}
`;

const StyledArrow = styled(ArrowRightOutlined)`
    color: ${ANTD_GRAY[8]};
    margin: 0 4px;
`;

type Props = {
    entities: Array<Entity>;
    onClick?: (index: number) => void;
    linkUrlParams?: Record<string, string | boolean>;
    showTooltips?: boolean;
    showArrows?: boolean;
};
export const CompactEntityNameList = ({ entities, onClick, linkUrlParams, showTooltips = true, showArrows }: Props) => {
    const entityRegistry = useEntityRegistry();
    const history = useHistory();

    return (
        <>
            {entities.map((mappedEntity, index) => {
                if (!mappedEntity) return <></>;
                let entity = mappedEntity;
                let columnName;
                if (entity.type === EntityType.SchemaField) {
                    const { parent, fieldPath } = entity as SchemaFieldEntity;
                    entity = parent;
                    columnName = fieldPath;
                }

                const isLastEntityInList = index === entities.length - 1;
                const showArrow = showArrows && !isLastEntityInList;
                const genericProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
                const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;
                const displayName = entityRegistry.getDisplayName(entity.type, entity);
                const fallbackIcon = entityRegistry.getIcon(entity.type, 12, IconStyleType.ACCENT);
                const url = entityRegistry.getEntityUrl(entity.type, entity.urn, linkUrlParams);
                return (
                    <NameWrapper addMargin={showArrow}>
                        <span
                            onClickCapture={(e) => {
                                // prevents the search links from taking over
                                e.preventDefault();
                                history.push(url);
                            }}
                        >
                            <HoverEntityTooltip entity={entity} canOpen={showTooltips}>
                                <span data-testid={`compact-entity-link-${entity.urn}`}>
                                    <EntityPreviewTag
                                        displayName={displayName}
                                        url={url}
                                        platformLogoUrl={platformLogoUrl || undefined}
                                        platformLogoUrls={genericProps?.siblingPlatforms?.map(
                                            (platform) => platform.properties?.logoUrl,
                                        )}
                                        logoComponent={fallbackIcon}
                                        onClick={() => onClick?.(index)}
                                        columnName={columnName}
                                    />
                                </span>
                            </HoverEntityTooltip>
                        </span>
                        {showArrow && <StyledArrow />}
                    </NameWrapper>
                );
            })}
        </>
    );
};
