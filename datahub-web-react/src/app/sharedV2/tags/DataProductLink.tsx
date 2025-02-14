import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { CloseOutlined } from '@ant-design/icons';
import { DataProduct as DataProductEntity, EntityType } from '../../../types.generated';
import { IconStyleType } from '../../entity/Entity';
import { HoverEntityTooltip } from '../../recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useEmbeddedProfileLinkProps } from '../../shared/useEmbeddedProfileLinkProps';

const DataProductLinkContainer = styled(Link)`
    display: inline-block;
`;

const DataProductWrapper = styled.span`
    display: inline-block;
    margin-bottom: 8px;
`;

const CloseButton = styled.div`
    margin-left: 4px;
    :hover {
        cursor: pointer;
    }
    && {
        color: ${ANTD_GRAY[7]};
    }
`;

const StyledCloseOutlined = styled(CloseOutlined)`
    && {
        font-size: 10px;
    }
`;

const IconWrapper = styled.span`
    margin-right: 6px;
`;

const StyledTag = styled.div<{ fontSize?: number }>`
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
    display: flex;
    align-items: center;
    justify-content: start;
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
            <IconWrapper>
                {entityRegistry.getIcon(EntityType.DataProduct, fontSize || 10, IconStyleType.ACCENT, ANTD_GRAY[9])}
            </IconWrapper>
            {displayName}
            {closable && (
                <CloseButton onClick={onClose}>
                    <StyledCloseOutlined />
                </CloseButton>
            )}
        </StyledTag>
    );
}

export type Props = {
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
