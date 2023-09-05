import { Tag } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { DataProduct, EntityType } from '../../../types.generated';
import { IconStyleType } from '../../entity/Entity';
import { HoverEntityTooltip } from '../../recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ANTD_GRAY } from '../../entity/shared/constants';

const DomainLinkContainer = styled(Link)`
    display: inline-block;
    margin-bottom: 8px;
`;

const DomainWrapper = styled.span`
    display: inline-block;
    margin-bottom: 8px;
`;

const StyledTag = styled(Tag)<{ fontSize?: number }>`
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
`;

interface ContentProps {
    dataProduct: DataProduct;
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
        <StyledTag style={tagStyle} closable={closable} onClose={onClose} fontSize={fontSize}>
            <span style={{ paddingRight: '4px' }}>
                {entityRegistry.getIcon(EntityType.DataProduct, fontSize || 10, IconStyleType.ACCENT, ANTD_GRAY[9])}
            </span>
            {displayName}
        </StyledTag>
    );
}

export type Props = {
    dataProduct: DataProduct;
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
    const urn = dataProduct?.urn;

    if (readOnly) {
        return (
            <HoverEntityTooltip entity={dataProduct}>
                <DomainWrapper>
                    <DataProductContent
                        dataProduct={dataProduct}
                        name={name}
                        closable={closable}
                        onClose={onClose}
                        tagStyle={tagStyle}
                        fontSize={fontSize}
                    />
                </DomainWrapper>
            </HoverEntityTooltip>
        );
    }

    return (
        <HoverEntityTooltip entity={dataProduct}>
            <DomainLinkContainer to={entityRegistry.getEntityUrl(EntityType.DataProduct, urn)}>
                <DataProductContent
                    dataProduct={dataProduct}
                    name={name}
                    closable={closable}
                    onClose={onClose}
                    tagStyle={tagStyle}
                    fontSize={fontSize}
                />
            </DomainLinkContainer>
        </HoverEntityTooltip>
    );
};
