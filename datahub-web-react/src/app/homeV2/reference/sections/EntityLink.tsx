import React from 'react';
import { Link } from 'react-router-dom';
import styled, { CSSObject } from 'styled-components';

import { Entity, EntityType } from '../../../../types.generated';
import { GenericEntityProperties } from '../../../entity/shared/types';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import PlatformIcon from '../../../sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { GlossaryPreviewCardDecoration } from '../../../entityV2/shared/containers/profile/header/GlossaryPreviewCardDecoration';

const Container = styled.div<{ showHover: boolean; entity: GenericEntityProperties }>`
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

const LinkButton = styled(Link)`
    padding: 2px 4px;
    height: auto;
    margin: 4px 0px 4px 0px;
    max-width: 100%; /* Ensure the grid container does not exceed its parent's width */
    overflow-x: hidden;

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
};

export const EntityLink = ({ entity, styles, render, displayTextStyle, onClick }: Props) => {
    const entityRegistry = useEntityRegistry();

    if (!entity?.urn || !entity.type) return null;

    const displayName = entityRegistry.getDisplayName(entity.type, entity);
    // const subType = entity?.subTypes?.typeNames?.[0];
    // const SubTypeIcon = subType && getSubTypeIcon(subType);
    // console.log(displayName, subType, SubTypeIcon, entity);

    const getPlatformIcon = (entityData: GenericEntityProperties) => {
        if (entityData.type === EntityType.GlossaryTerm) {
            return (
                <RibbonDecoration>
                    <GlossaryPreviewCardDecoration urn={entity.urn || ''} entityData={entity} />
                </RibbonDecoration>
            );
        }
        return entityData?.platform ? (
            <PlatformIcon
                platform={entityData?.platform}
                size={17}
                styles={{
                    padding: '4px',
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
                <HoverEntityTooltip entity={entity as Entity} showArrow={false} placement="bottom">
                    <LinkButton
                        to={!onClick ? entityRegistry.getEntityUrl(entity.type, entity.urn) : undefined}
                        onClick={onClick}
                    >
                        {getPlatformIcon(entity)}
                        <DisplayNameText entity={entity} style={{ ...displayTextStyle }}>
                            {displayName}
                        </DisplayNameText>
                    </LinkButton>
                </HoverEntityTooltip>
            )}
        </Container>
    );
};
