import Icon from '@ant-design/icons/lib/components/Icon';
import { getSchemaFieldParentLink } from '@src/app/entityV2/schemaField/utils';
import { CompactEntityNameComponent } from '@src/app/recommendations/renderer/component/CompactEntityNameComponent';
import { Entity, EntityType } from '@src/types.generated';
import { Typography } from 'antd';
import React from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';
import ExternalLink from '../../../../../images/link-out.svg?react';
import EntityIcon from '../../../../entity/shared/components/styled/EntityIcon';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { ANTD_GRAY } from '../../constants';
import CompactMarkdownViewer from '../Documentation/components/CompactMarkdownViewer';
import { ValueColumnData } from './types';

const ValueText = styled(Typography.Text)<{ size: number }>`
    font-family: 'Manrope';
    font-weight: 400;
    font-size: ${(props) => props.size}px;
    color: ${ANTD_GRAY[9]};
    display: block;
    width: 100%;
    .remirror-editor.ProseMirror {
        font-size: ${(props) => props.size}px;
    }
`;

const StyledIcon = styled(Icon)`
    margin-left: 6px;
`;

const IconWrapper = styled.span`
    margin-right: 4px;
    display: flex;
`;

const EntityWrapper = styled.div`
    display: flex;
    align-items: center;
`;

const EntityName = styled(Typography.Text)`
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
`;

const StyledHighlight = styled(Highlight)<{ truncateText?: boolean }>`
    line-height: 1.5;
    text-wrap: wrap;

    ${(props) =>
        props.truncateText &&
        `
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        max-width: 100%;
        display: block;
    `}
`;

interface Props {
    value: ValueColumnData;
    isRichText?: boolean;
    filterText?: string;
    truncateText?: boolean;
    isFieldColumn?: boolean;
    size?: number;
    hydratedEntityMap?: Record<string, Entity>;
}

export default function StructuredPropertyValue({
    value,
    isRichText,
    filterText,
    truncateText,
    isFieldColumn,
    size = 12,
    hydratedEntityMap,
}: Props) {
    const entityRegistry = useEntityRegistry();

    const getEntityLink = (entity: Entity) =>
        entity.type === EntityType.SchemaField
            ? getSchemaFieldParentLink(entity.urn)
            : entityRegistry.getEntityUrl(entity.type, entity.urn);

    let valueEntityRender = <></>;
    if (value.entity) {
        if (hydratedEntityMap && hydratedEntityMap[value.entity.urn]) {
            valueEntityRender = (
                <CompactEntityNameComponent entity={hydratedEntityMap[value.entity.urn]} showFullTooltip />
            );
        } else {
            valueEntityRender = (
                <EntityWrapper>
                    <IconWrapper>
                        <EntityIcon entity={value.entity} size={size} />
                    </IconWrapper>
                    <EntityName ellipsis={{ tooltip: true }}>
                        {entityRegistry.getDisplayName(value.entity.type, value.entity)}
                    </EntityName>
                    <Typography.Link href={getEntityLink(value.entity)} target="_blank" rel="noopener noreferrer">
                        <StyledIcon component={ExternalLink} />
                    </Typography.Link>
                </EntityWrapper>
            );
        }
    }

    return (
        <ValueText size={size}>
            {value.entity ? (
                valueEntityRender
            ) : (
                <>
                    {isRichText ? (
                        <CompactMarkdownViewer
                            content={value.value?.toString() ?? ''}
                            lineLimit={isFieldColumn ? 1 : undefined}
                            hideShowMore={isFieldColumn}
                            scrollableY={!isFieldColumn}
                        />
                    ) : (
                        <>
                            {truncateText ? (
                                <Typography.Text ellipsis={{ tooltip: true }}>
                                    {value.value?.toString() || <div style={{ minHeight: 22 }} />}
                                </Typography.Text>
                            ) : (
                                <StyledHighlight search={filterText} truncateText={truncateText}>
                                    {value.value?.toString() || <div style={{ minHeight: 22 }} />}
                                </StyledHighlight>
                            )}
                        </>
                    )}
                </>
            )}
        </ValueText>
    );
}
