import Icon from '@ant-design/icons/lib/components/Icon';
import React, { useState } from 'react';
import Highlight from 'react-highlighter';
import { Button, Typography } from 'antd';
import styled from 'styled-components';
import { ValueColumnData } from './types';
import { ANTD_GRAY } from '../../constants';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import ExternalLink from '../../../../../images/link-out.svg?react';
import MarkdownViewer, { MarkdownView } from '../../components/legacy/MarkdownViewer';
import EntityIcon from '../../components/styled/EntityIcon';

const ValueText = styled(Typography.Text)`
    font-family: 'Manrope';
    font-weight: 400;
    font-size: 14px;
    color: ${ANTD_GRAY[9]};
    display: block;

    ${MarkdownView} {
        font-size: 14px;
    }
`;

const StyledIcon = styled(Icon)`
    margin-left: 6px;
`;

const IconWrapper = styled.span`
    margin-right: 4px;
`;

const StyledButton = styled(Button)`
    margin-top: 2px;
`;

interface Props {
    value: ValueColumnData;
    isRichText?: boolean;
    filterText?: string;
}

const MAX_CHARACTERS = 200;

export default function StructuredPropertyValue({ value, isRichText, filterText }: Props) {
    const entityRegistry = useEntityRegistry();
    const [showMore, setShowMore] = useState(false);

    const toggleShowMore = () => {
        setShowMore(!showMore);
    };

    const valueAsString = value?.value?.toString() ?? '';

    return (
        <ValueText>
            {value.entity ? (
                <>
                    <IconWrapper>
                        <EntityIcon entity={value.entity} />
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
                        <MarkdownViewer source={value.value as string} />
                    ) : (
                        <>
                            <Highlight search={filterText}>
                                {showMore ? valueAsString : valueAsString?.substring(0, MAX_CHARACTERS)}
                            </Highlight>
                            {valueAsString?.length > MAX_CHARACTERS && (
                                <StyledButton type="link" onClick={toggleShowMore}>
                                    {showMore ? 'Show less' : 'Show more'}
                                </StyledButton>
                            )}
                        </>
                    )}
                </>
            )}
        </ValueText>
    );
}
