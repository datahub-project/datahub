import Icon from '@ant-design/icons/lib/components/Icon';
import { Typography } from 'antd';
import React from 'react';
import Highlight from 'react-highlighter';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import EntityIcon from '@app/entity/shared/components/styled/EntityIcon';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import CompactMarkdownViewer from '@app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';
import { ValueColumnData } from '@app/entityV2/shared/tabs/Properties/types';
import HoverCardAttributionDetails from '@app/sharedV2/propagation/HoverCardAttributionDetails';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Tooltip, colors } from '@src/alchemy-components';
import { getSchemaFieldParentLink } from '@src/app/entityV2/schemaField/utils';
import { CompactEntityNameComponent } from '@src/app/recommendations/renderer/component/CompactEntityNameComponent';
import { Entity, EntityType, MetadataAttribution } from '@src/types.generated';

import ExternalLink from '@images/link-out.svg?react';

const ValueText = styled(Typography.Text)<{ size: number; $isProposed?: boolean }>`
    font-family: 'Manrope';
    font-weight: 400;
    font-size: ${(props) => props.size}px;
    color: ${ANTD_GRAY[9]};
    display: block;
    width: 100%;
    .remirror-editor.ProseMirror {
        font-size: ${(props) => props.size}px;
    }

    ${(props) =>
        props.$isProposed &&
        `
            :hover {
                cursor: pointer;
        }
        `}
`;

const StyledIcon = styled(Icon)`
    margin-left: 6px;
`;

const IconWrapper = styled.span`
    margin-right: 4px;
    display: flex;
`;

const EntityWrapper = styled.div<{ $isProposed?: boolean }>`
    display: flex;
    align-items: center;
    ${(props) =>
        props.$isProposed &&
        `
    border: 1px dashed ${colors.gray[200]};
    padding: 2px 4px;
    margin: 2px 0;
    border-radius: 200px;
    background-color: ${colors.white};
    `}
`;

const BorderedContainer = styled.div<{ $isProposed?: boolean; $isStraightBorder?: boolean }>`
    span {
        color: ${colors.gray[500]};
    }

    ${(props) =>
        props.$isProposed &&
        `
        display: inline-flex;
        border: 1px dashed ${colors.gray[200]};
        padding: 2px 6px;
        margin: 2px 0;
        border-radius:  ${props.$isStraightBorder ? '8px' : '200px'};
        background-color: ${colors.white};
        max-width: 100%;
        `}
`;

const Container = styled.div`
    display: inline-flex;
    max-width: 100%;
    width: 100%;
`;

const ViewerContainer = styled.div`
    max-width: calc(100% - 16px);
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
    attribution?: MetadataAttribution | null;
}

export default function StructuredPropertyValue({
    value,
    isRichText,
    filterText,
    truncateText,
    isFieldColumn,
    size = 12,
    hydratedEntityMap,
    attribution,
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
                    <Link to={getEntityLink(value.entity)} target="_blank" rel="noopener noreferrer">
                        <StyledIcon component={ExternalLink} />
                    </Link>
                </EntityWrapper>
            );
        }
    }

    return (
        <Tooltip title={attribution && <HoverCardAttributionDetails propagationDetails={{ attribution }} />}>
            <ValueText size={size}>
                {value.entity ? (
                    valueEntityRender
                ) : (
                    <BorderedContainer $isStraightBorder={isRichText && !isFieldColumn}>
                        {isRichText ? (
                            <Container>
                                <ViewerContainer>
                                    <CompactMarkdownViewer
                                        content={value.value?.toString() ?? ''}
                                        lineLimit={isFieldColumn ? 1 : undefined}
                                        hideShowMore={isFieldColumn}
                                        scrollableY={!isFieldColumn}
                                    />
                                </ViewerContainer>
                            </Container>
                        ) : (
                            <>
                                {truncateText ? (
                                    <Typography.Text
                                        ellipsis={{ tooltip: attribution ? { placement: 'bottom' } : true }}
                                    >
                                        {value.value?.toString() || <div style={{ minHeight: 22 }} />}
                                    </Typography.Text>
                                ) : (
                                    <StyledHighlight search={filterText} truncateText={truncateText}>
                                        {value.value?.toString() || <div style={{ minHeight: 22 }} />}
                                    </StyledHighlight>
                                )}
                            </>
                        )}
                    </BorderedContainer>
                )}
            </ValueText>
        </Tooltip>
    );
}
