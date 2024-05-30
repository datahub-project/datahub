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
import EntityIcon from '../../../../entity/shared/components/styled/EntityIcon';

const ValueText = styled(Typography.Text)`
    font-family: 'Manrope';
    font-weight: 400;
    font-size: 12px;
    color: ${ANTD_GRAY[9]};
    display: block;

    .remirror-editor.ProseMirror {
        font-size: 12px;
    }
`;

const StyledIcon = styled(Icon)`
    margin-left: 6px;
`;

const IconWrapper = styled.span`
    margin-right: 4px;
`;

const StyledHighlight = styled(Highlight)`
    line-height: 1.5;
    text-wrap: wrap;
`;

interface Props {
    value: ValueColumnData;
    isRichText?: boolean;
    filterText?: string;
}

export default function StructuredPropertyValue({ value, isRichText, filterText }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <ValueText>
            {value.entity ? (
                <>
                    <IconWrapper>
                        <EntityIcon entity={value.entity} size={12} />
                    </IconWrapper>
                    {entityRegistry.getDisplayName(value.entity.type, value.entity)}
                    <Typography.Link
                        href={entityRegistry.getEntityUrl(value.entity.type, value.entity.urn)}
                        target="_blank"
                        rel="noopener noreferrer"
                    >
                        <StyledIcon component={ExternalLink} />
                    </Typography.Link>
                </>
            ) : (
                <>
                    {isRichText ? (
                        <CompactMarkdownViewer content={value.value?.toString() ?? ''} />
                    ) : (
                        <StyledHighlight search={filterText}>
                            {value.value?.toString() || <div style={{ minHeight: 22 }} />}
                        </StyledHighlight>
                    )}
                </>
            )}
        </ValueText>
    );
}
