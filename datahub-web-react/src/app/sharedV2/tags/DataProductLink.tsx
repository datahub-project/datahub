import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { useEmbeddedProfileLinkProps } from '@app/shared/useEmbeddedProfileLinkProps';
import PillRemoveIcon from '@app/sharedV2/icons/PillRemoveIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataProduct as DataProductEntity, EntityType } from '@types';

const DataProductLinkContainer = styled(Link)`
    display: inline-block;
    color: ${(props) => props.theme.colors.text};

    &:hover,
    &:focus,
    &:active {
        color: ${(props) => props.theme.colors.text};
    }
`;

const DataProductWrapper = styled.span`
    display: inline-block;
    margin-bottom: 8px;
`;

const RemoveIcon = styled(PillRemoveIcon)`
    margin-left: 4px;
`;

const IconWrapper = styled.div`
    display: flex;
    color: ${(props) => props.theme.colors.icon};
`;

const StyledTag = styled.div<{ fontSize?: number }>`
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
    font-weight: 500;
    display: flex;
    align-items: center;
    justify-content: start;
    gap: 4px;
`;

interface ContentProps {
    dataProduct: DataProductEntity;
    name?: string;
    closable?: boolean;
    onClose?: (e: any) => void;
    tagStyle?: any | undefined;
    fontSize?: number;
}

function DataProductContent({ dataProduct, name, closable, onClose, tagStyle, fontSize }: ContentProps) {
    const entityRegistry = useEntityRegistry();

    const displayName = name || entityRegistry.getDisplayName(EntityType.DataProduct, dataProduct);

    return (
        <StyledTag style={tagStyle} fontSize={fontSize}>
            <IconWrapper>{entityRegistry.getIcon(EntityType.DataProduct, 16, IconStyleType.ACCENT)}</IconWrapper>
            {displayName}
            {closable && <RemoveIcon onClick={onClose} />}
        </StyledTag>
    );
}

type Props = {
    dataProduct: DataProductEntity;
    name?: string;
    closable?: boolean;
    onClose?: (e: any) => void;
    tagStyle?: any | undefined;
    readOnly?: boolean;
    fontSize?: number;
};

export const DataProductLink = ({
    dataProduct,
    name,
    closable,
    onClose,
    tagStyle,
    readOnly,
    fontSize,
}: Props): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const linkProps = useEmbeddedProfileLinkProps();
    const urn = dataProduct?.urn;

    if (readOnly) {
        return (
            <HoverEntityTooltip entity={dataProduct}>
                <DataProductWrapper>
                    <DataProductContent
                        dataProduct={dataProduct}
                        name={name}
                        closable={closable}
                        onClose={onClose}
                        tagStyle={tagStyle}
                        fontSize={fontSize}
                    />
                </DataProductWrapper>
            </HoverEntityTooltip>
        );
    }

    return (
        <HoverEntityTooltip entity={dataProduct}>
            <DataProductLinkContainer to={entityRegistry.getEntityUrl(EntityType.DataProduct, urn)} {...linkProps}>
                <DataProductContent
                    dataProduct={dataProduct}
                    name={name}
                    closable={closable}
                    onClose={onClose}
                    tagStyle={tagStyle}
                    fontSize={fontSize}
                />
            </DataProductLinkContainer>
        </HoverEntityTooltip>
    );
};
