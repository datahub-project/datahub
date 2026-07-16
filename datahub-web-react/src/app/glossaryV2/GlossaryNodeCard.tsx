import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components/macro';

import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { Card, Tooltip } from '@src/alchemy-components';

import { DisplayProperties } from '@types';

const GlossaryItemCount = styled.span<{ count: number }>`
    display: flex;
    align-items: center;
    gap: 5px;
    border-radius: 20px;
    background: ${(props) => props.theme.colors.bgSurface};
    color: ${(props) => (props.count > 0 ? props.theme.colors.textSecondary : props.theme.colors.textDisabled)};
    padding: 5px 10px;
    width: max-content;
    svg {
        height: 14px;
        width: 14px;
        path {
            fill: ${(props) => (props.count > 0 ? props.theme.colors.icon : props.theme.colors.iconDisabled)};
        }
    }
    border: 1px solid transparent;
    :hover {
        border: 1px solid ${(props) => (props.count > 0 ? props.theme.colors.borderHover : 'transparent')};
    }
`;

const CountText = styled.span`
    font-size: 10px;
    font-weight: 400;
`;

const Icons = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 5px;
`;

const CardDescription = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    line-height: normal;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
    text-overflow: ellipsis;
    word-break: break-word;
    overflow-wrap: anywhere;
`;

interface Props {
    name: string;
    description: string | undefined;
    termCount: number;
    nodeCount: number;
    displayProperties?: Maybe<DisplayProperties>;
    urn: string;
    maxDepth?: number;
}

const MAX_DEPTH_QUERIED = 4;

const GlossaryNodeCard = (props: Props) => {
    const { t } = useTranslation('governance.glossary');
    const { name, description, termCount, nodeCount, displayProperties, urn, maxDepth } = props;
    const generateColor = useGenerateGlossaryColorFromPalette();
    const glossaryColor = displayProperties?.colorHex || generateColor(urn);
    const theme = useTheme();

    const isExceedingMaxDepth = (maxDepth || 0) > MAX_DEPTH_QUERIED;

    return (
        <Card
            title={name}
            icon={<GlossaryColoredIcon color={glossaryColor} icon={BookmarksSimple} size={40} iconSize={22} />}
            iconAlignment="horizontal"
            onClick={() => {}}
            style={{ overflow: 'hidden', width: '100%', height: '100%', minWidth: 0 }}
        >
            {description ? (
                <Tooltip
                    title={description}
                    placement="top"
                    showArrow={false}
                    overlayStyle={{ maxWidth: 320, borderRadius: '12px' }}
                    overlayInnerStyle={{
                        color: theme.colors.textSecondary,
                        wordBreak: 'break-word',
                        overflowWrap: 'anywhere',
                    }}
                >
                    <CardDescription>{description}</CardDescription>
                </Tooltip>
            ) : (
                /* eslint-disable i18next/no-literal-string -- (untranslated-text) placeholder dash, not translatable prose */
                <CardDescription>--</CardDescription>
                /* eslint-enable i18next/no-literal-string */
            )}
            <Icons>
                <Tooltip
                    title={t('card.nodeGroupCountTooltip', { count: nodeCount })}
                    placement="top"
                    showArrow={false}
                >
                    <GlossaryItemCount count={nodeCount}>
                        <BookmarksSimple />
                        <CountText>
                            {' '}
                            {nodeCount}
                            {isExceedingMaxDepth && `+`}
                        </CountText>
                    </GlossaryItemCount>
                </Tooltip>
                <Tooltip title={t('card.termCountTooltip', { count: termCount })} placement="top" showArrow={false}>
                    <GlossaryItemCount count={termCount}>
                        <BookmarkSimple />
                        <CountText>
                            {' '}
                            {termCount}
                            {isExceedingMaxDepth && `+`}
                        </CountText>
                    </GlossaryItemCount>
                </Tooltip>
            </Icons>
        </Card>
    );
};

export default GlossaryNodeCard;
