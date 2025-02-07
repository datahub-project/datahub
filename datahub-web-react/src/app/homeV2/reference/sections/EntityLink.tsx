import React from 'react';
import { Link } from 'react-router-dom';
import styled, { CSSObject } from 'styled-components';
import HealthIcon from '@src/app/previewV2/HealthIcon';
import { useEmbeddedProfileLinkProps } from '@src/app/shared/useEmbeddedProfileLinkProps';
import PlatformHeaderIcons from '@src/app/entityV2/shared/containers/profile/header/PlatformContent/PlatformHeaderIcons';
import { getEntityPlatforms } from '@src/app/entityV2/shared/containers/profile/header/utils';
import { Entity, EntityType } from '../../../../types.generated';
import { GenericEntityProperties } from '../../../entity/shared/types';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { GlossaryPreviewCardDecoration } from '../../../entityV2/shared/containers/profile/header/GlossaryPreviewCardDecoration';

const Container = styled.div<{ showHover: boolean; entity: GenericEntityProperties }>`
    display: flex;
    justify-content: space-between;
    align-items: center;

    overflow: hidden;
    border-radius: 8px;
    cursor: pointer;
    width: ${(props) => props.entity.type === EntityType.GlossaryTerm && 'fit-content'};
    border: ${(props) => (props.entity.type === EntityType.GlossaryTerm ? '1px solid #C1C4D0' : 'none')};

    :hover {
        ${(props) => props.showHover && 'background-color: #f5f7fa;'}
    }

    > a {
        margin: ${(props) => props.entity.type === EntityType.GlossaryTerm && '0px'};
    }
`;

const IconWrapper = styled.div`
    padding-right: 4px;
`;

const LinkButton = styled(Link)<{ includePadding: boolean }>`
    padding: ${(props) => (props.includePadding ? '2px 4px' : '0px')};
    height: auto;
    margin: 4px 0px 4px 0px;
    max-width: 100%; /* Ensure the grid container does not exceed its parent's width */
    overflow-x: hidden;
    width: 100%;

    &&& {
        display: flex;
        align-items: center;
        gap: 4px;
    }
`;

const DisplayNameText = styled.span<{ entity: GenericEntityProperties }>`
    color: #52596c;
    font-family: Mulish;
    font-size: 12px;
    font-style: normal;
    font-weight: 600;
    line-height: normal;
    padding: ${(props) => props.entity.type === EntityType.GlossaryTerm && '8px 10px 8px 0px'};

    overflow: hidden;
    text-overflow: ellipsis;
`;

const RibbonDecoration = styled.div`
    width: 22px;
    height: 32px;
    position: relative;
    overflow: hidden;

    > span {
        top: -10px;
        padding: 5px;
    }
`;

type Props = {
    entity: GenericEntityProperties | null;
    styles?: CSSObject;
    displayTextStyle?: CSSObject;
    render?: (entity: GenericEntityProperties) => React.ReactNode;
    onClick?: (e) => void;
    showHealthIcon?: boolean;
};

export const EntityLink = ({ entity, styles, render, displayTextStyle, onClick, showHealthIcon = false }: Props) => {
    const entityRegistry = useEntityRegistry();
    const linkProps = useEmbeddedProfileLinkProps();

    if (!entity?.urn || !entity.type) return null;

    const displayName = entityRegistry.getDisplayName(entity.type, entity);

    const getPlatformIcon = (entityData: GenericEntityProperties) => {
        if (entityData.type === EntityType.GlossaryTerm) {
            return (
                <RibbonDecoration>
                    <GlossaryPreviewCardDecoration urn={entity.urn || ''} entityData={entity} />
                </RibbonDecoration>
            );
        }
        const { platform, platforms } = getEntityPlatforms(entityData.type || null, entityData);
        return platform || !!platforms?.length ? (
            <PlatformHeaderIcons
                platform={platform || undefined}
                platforms={platforms || undefined}
                size={17}
                styles={{
                    padding: '4px',
                    borderRadius: '8px',
                    ...styles,
                }}
            />
        ) : null;
    };

    return (
        <Container showHover={!render} entity={entity}>
            {render ? (
                render(entity)
            ) : (
                <>
                    <HoverEntityTooltip entity={entity as Entity} showArrow={false} placement="bottom">
                        <LinkButton
                            includePadding={entity.type !== EntityType.GlossaryTerm}
                            to={!onClick ? entityRegistry.getEntityUrl(entity.type, entity.urn) : undefined}
                            onClick={onClick}
                            {...linkProps}
                        >
                            {getPlatformIcon(entity)}
                            <DisplayNameText entity={entity} style={{ ...displayTextStyle }}>
                                {displayName}
                            </DisplayNameText>
                        </LinkButton>
                    </HoverEntityTooltip>
                    {entity?.health && showHealthIcon && (
                        <IconWrapper>
                            <HealthIcon
                                urn={entity?.urn}
                                health={entity.health}
                                baseUrl={entityRegistry.getEntityUrl(entity.type, entity.urn)}
                            />
                        </IconWrapper>
                    )}
                </>
            )}
        </Container>
    );
};
