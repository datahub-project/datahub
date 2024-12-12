import Icon from '@ant-design/icons/lib/components/Icon';
import React from 'react';
import Highlight from 'react-highlighter';
import { Typography } from 'antd';
import styled from 'styled-components';
import { ValueColumnData } from './types';
import { ANTD_GRAY } from '../../constants';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import ExternalLink from '../../../../../images/link-out.svg?react';
import CompactMarkdownViewer from '../Documentation/components/CompactMarkdownViewer';
import EntityIcon from '../../components/styled/EntityIcon';

const ValueText = styled(Typography.Text)`
    font-family: 'Manrope';
    font-weight: 400;
    font-size: 12px;
    color: ${ANTD_GRAY[9]};
    display: block;
    width: 100%;
    margin-bottom: 2px;

    .remirror-editor.ProseMirror {
        font-size: 12px;
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
}

export default function StructuredPropertyValue({ value, isRichText, filterText, truncateText, isFieldColumn }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <ValueText>
            {value.entity ? (
                <EntityWrapper>
                    <IconWrapper>
                        <EntityIcon entity={value.entity} size={12} />
                    </IconWrapper>
                    <EntityName ellipsis={{ tooltip: true }}>
                        {entityRegistry.getDisplayName(value.entity.type, value.entity)}
                    </EntityName>
                    <Typography.Link
                        href={entityRegistry.getEntityUrl(value.entity.type, value.entity.urn)}
                        target="_blank"
                        rel="noopener noreferrer"
                    >
                        <StyledIcon component={ExternalLink} />
                    </Typography.Link>
                </EntityWrapper>
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
