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

interface ContentProps {
    dataProduct: DataProduct;
    name?: string;
    closable?: boolean;
    onClose?: (e: any) => void;
    tagStyle?: any | undefined;
}

function DataProductContent({ dataProduct, name, closable, onClose, tagStyle }: ContentProps) {
    const entityRegistry = useEntityRegistry();

    const displayName = name || entityRegistry.getDisplayName(EntityType.DataProduct, dataProduct);

    return (
        <Tag style={tagStyle} closable={closable} onClose={onClose}>
            <span style={{ paddingRight: '4px' }}>
                {entityRegistry.getIcon(EntityType.DataProduct, 10, IconStyleType.ACCENT, ANTD_GRAY[9])}
            </span>
            {displayName}
        </Tag>
    );
}

export type Props = {
    dataProduct: DataProduct;
    name?: string;
    closable?: boolean;
    onClose?: (e: any) => void;
    tagStyle?: any | undefined;
    readOnly?: boolean;
};

export const DataProductLink = ({ dataProduct, name, closable, onClose, tagStyle, readOnly }: Props): JSX.Element => {
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
                />
            </DomainLinkContainer>
        </HoverEntityTooltip>
    );
};
