import React from 'react';
import { Link } from 'react-router-dom';
import styled, { CSSObject } from 'styled-components';

import { Entity, EntityType } from '../../../../types.generated';
import { GenericEntityProperties } from '../../../entity/shared/types';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '../../../useEntityRegistry';
import PlatformIcon from '../../../sharedV2/icons/PlatformIcon';

const Container = styled.div<{ showHover: boolean }>`
    overflow: hidden;
    border-radius: 5px;
    cursor: pointer;
    :hover {
        ${(props) => props.showHover && 'background-color: #f5f7fa;'}
    }
`;

const LinkButton = styled(Link)`
    padding: 0px;
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

const DisplayNameText = styled.span`
    color: #52596c;
    font-family: Mulish;
    font-size: 12px;
    font-style: normal;
    font-weight: 600;
    line-height: normal;
`;

type Props = {
    entity: GenericEntityProperties;
    styles?: CSSObject;
    displayTextStyle?: CSSObject;
    render?: (entity: GenericEntityProperties) => React.ReactNode;
};

export const EntityLink = ({ entity, styles, render, displayTextStyle }: Props) => {
    const entityRegistry = useEntityRegistry();

    if (!entity.urn || !entity.type) return null;

    const displayName = entityRegistry.getDisplayName(entity.type as EntityType, entity);
    // const subType = entity?.subTypes?.typeNames?.[0];
    // const SubTypeIcon = subType && getSubTypeIcon(subType);
    // console.log(displayName, subType, SubTypeIcon, entity);

    const defaultRender = () => {
        return (
            <HoverEntityTooltip entity={entity as Entity} showArrow={false} placement="bottom">
                <LinkButton to={entityRegistry.getEntityUrl(entity.type as EntityType, entity.urn as string)}>
                    {/* {SubTypeIcon && <SubTypeIcon style={{ marginRight: '4px' }} />} */}
                    <PlatformIcon
                        platform={entity?.platform}
                        size={17}
                        styles={{
                            padding: '4px',
                            ...styles,
                        }}
                    />
                    <DisplayNameText style={{ ...displayTextStyle }}>{displayName}</DisplayNameText>
                </LinkButton>
            </HoverEntityTooltip>
        );
    };

    return <Container showHover={!render}>{render ? render(entity) : defaultRender()}</Container>;
};
