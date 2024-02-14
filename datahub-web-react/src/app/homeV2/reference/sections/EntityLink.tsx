import ColorThief from 'colorthief';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { Entity, EntityType } from '../../../../types.generated';
import { IconStyleType } from '../../../entity/Entity';
import { GenericEntityProperties } from '../../../entityV2/shared/types';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '../../../useEntityRegistry';

const Container = styled.div<{ showHover: boolean }>`
    overflow: hidden;
    border-radius: 5px;
    cursor: pointer;
    :hover {
        ${(props) => props.showHover && 'background-color: #f5f7fa;'}
    }
`;

const PreviewImage = styled.img`
    max-height: 17px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
    margin-left: 4px;
`;

const Icon = styled.div<{ background?: string }>`
    margin-left: 4px;
    width: 25px;
    height: 25px;
    margin-right: 4px;
    display: flex;
    background-color: ${({ background }) => background || 'transparent'};
    align-items: center;
    border-radius: 4px;
`;

const LinkButton = styled(Link)`
    padding: 0px;
    height: auto;
    margin: 4px 0px 4px 0px;
    &&& {
        display: flex;
        align-items: center;
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
    render?: (entity: GenericEntityProperties) => React.ReactNode;
};

export const EntityLink = ({ entity, render }: Props) => {
    const entityRegistry = useEntityRegistry();
    const imgRef = React.useRef<HTMLImageElement>(null);
    const [platformBackground, setPlatformBackground] = React.useState<string | undefined>(undefined);

    if (!entity.urn || !entity.type) return null;

    const displayName = entityRegistry.getDisplayName(entity.type as EntityType, entity);
    // const subType = entity?.subTypes?.typeNames?.[0];
    // const SubTypeIcon = subType && getSubTypeIcon(subType);
    // console.log(displayName, subType, SubTypeIcon, entity);

    const logo = entity.platform?.properties?.logoUrl ? (
        <PreviewImage
            src={entity.platform?.properties?.logoUrl || undefined}
            alt={displayName}
            ref={imgRef}
            onLoad={() => {
                const colorThief = new ColorThief();
                const img = imgRef.current;
                if (img) {
                    img.crossOrigin = 'anonymous';
                }
                const result = colorThief.getColor(img, 25);
                if (platformBackground) return;

                setPlatformBackground(`rgb(${result[0]}, ${result[1]}, ${result[2]}, .1)`);
            }}
        />
    ) : (
        entityRegistry.getIcon(entity.type as EntityType, 12, IconStyleType.ACCENT)
    );

    const defaultRender = () => {
        return (
            <HoverEntityTooltip entity={entity as Entity} showArrow={false} placement="bottom">
                <LinkButton to={entityRegistry.getEntityUrl(entity.type as EntityType, entity.urn as string)}>
                    {/* {SubTypeIcon && <SubTypeIcon style={{ marginRight: '4px' }} />} */}
                    <Icon background={platformBackground}>{logo}</Icon>
                    <DisplayNameText>{displayName}</DisplayNameText>
                </LinkButton>
            </HoverEntityTooltip>
        );
    };

    return <Container showHover={!render}>{render ? render(entity) : defaultRender()}</Container>;
};
