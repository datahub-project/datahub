import React from 'react';
import { Tag, Image } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { Entity } from '../../../types.generated';
import { IconStyleType } from '../../entity/Entity';
import { useEntityRegistry } from '../../useEntityRegistry';

type Props = {
    entities: Array<Entity>;
};

const EntityTag = styled(Tag)`
    margin: 4px;
`;

const TitleContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 4px;
`;

const IconContainer = styled.span`
    padding-left: 4px;
    padding-right: 4px;
    display: flex;
    align-items: center;
`;

const PlatformLogo = styled(Image)`
    max-height: 16px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const DisplayNameContainer = styled.span`
    padding-left: 4px;
    padding-right: 4px;
`;

export const EntityLinkGroup = ({ entities }: Props) => {
    const entityRegistry = useEntityRegistry();
    return (
        <>
            {entities.map((entity) => (
                <Link to={entityRegistry.getEntityUrl(entity.type, entity.urn)}>
                    <EntityTag>
                        <TitleContainer>
                            <IconContainer>
                                {(entityRegistry.getPlatformLogoUrl(entity.type, entity) && (
                                    <PlatformLogo
                                        preview={false}
                                        src={entityRegistry.getPlatformLogoUrl(entity.type, entity)}
                                        alt="none"
                                    />
                                )) ||
                                    entityRegistry.getIcon(entity.type, 12, IconStyleType.ACCENT)}
                            </IconContainer>
                            <DisplayNameContainer>
                                {entityRegistry.getDisplayName(entity.type, entity)}
                            </DisplayNameContainer>
                        </TitleContainer>
                    </EntityTag>
                </Link>
            ))}
        </>
    );
};
