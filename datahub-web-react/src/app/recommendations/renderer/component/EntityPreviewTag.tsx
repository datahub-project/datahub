import { Divider, Tooltip } from '@components';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useEmbeddedProfileLinkProps } from '@src/app/sharedV2/useEmbeddedProfileLinkProps';

// Styled to match the rest of the app's pills (see TagPill / GlossaryTermPill): 26px tall, fully
// rounded, single 8px horizontal padding, 12px / 500 text, theme-text color. A local element rather
// than alchemy Pill, which only takes a string label and can't hold the logos and tooltips below.
// font-size and font-weight are pinned explicitly so the pill text doesn't shift if an ancestor
// (e.g. sidebar headers) sets a different size or weight.
const EntityTag = styled.span<{ $showMargin?: boolean }>`
    box-sizing: border-box;
    white-space: nowrap;
    background-color: ${(props) => props.theme.colors.bgSurface};
    border: 1px solid ${(props) => props.theme.colors.border};
    ${(props) => (props.$showMargin ? `margin: 4px;` : `margin: 0px;`)}
    display: inline-flex;
    align-items: center;
    height: 26px;
    padding: 0 8px;
    border-radius: 100em;
    line-height: 1.4;
    font-size: 12px;
    font-weight: 500;
    color: ${(props) => props.theme.colors.text};
    max-width: inherit;
`;

const TitleContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    max-width: inherit;
`;

const IconContainer = styled.span`
    display: flex;
    align-items: center;
`;

const PlatformLogo = styled.img`
    vertical-align: middle;
    max-height: 16px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const DisplayNameContainer = styled.span`
    overflow: hidden;
    display: flex;
    align-items: center;
    span {
        overflow: hidden;
        text-overflow: ellipsis;
    }
`;

const ColumnName = styled.span`
    color: ${(props) => props.theme.colors.textSecondary};
    font-family: 'Roboto Mono', monospace;
    font-weight: bold;
`;

const StyledDivider = styled(Divider)`
    background-color: ${(props) => props.theme.colors.border};
    margin: 0 4px;
`;

const StyledLink = styled(Link)`
    max-width: inherit;
`;

type Props = {
    displayName: string;
    url: string;
    platformLogoUrl?: string;
    platformLogoUrls?: Maybe<string>[];
    logoComponent?: React.ReactNode;
    onClick?: () => void;
    columnName?: string;
    dataTestId?: string;
    showNameTooltip?: boolean;
    showMargin?: boolean;
};

const constructExternalUrl = (url) => {
    if (!url) return null;

    const [baseUrl, queryString] = url.split('?');
    const newBaseUrl = `${baseUrl}/`;

    return queryString ? `${newBaseUrl}?${queryString}` : newBaseUrl;
};

export const EntityPreviewTag = ({
    displayName,
    url,
    platformLogoUrl,
    platformLogoUrls,
    logoComponent,
    onClick,
    columnName,
    dataTestId,
    showNameTooltip = true,
    showMargin = true,
}: Props) => {
    const externalUrl = constructExternalUrl(url);
    const linkProps = useEmbeddedProfileLinkProps();

    return (
        <StyledLink to={externalUrl} {...linkProps} onClick={onClick} data-testid={dataTestId}>
            <EntityTag $showMargin={showMargin}>
                <TitleContainer>
                    <IconContainer>
                        {(!!platformLogoUrl && !platformLogoUrls && <PlatformLogo src={platformLogoUrl} alt="" />) ||
                            (!!platformLogoUrls &&
                                platformLogoUrls.slice(0, 2).map((platformLogoUrlsEntry) => (
                                    <>
                                        <PlatformLogo src={platformLogoUrlsEntry || ''} alt="" />
                                    </>
                                ))) ||
                            logoComponent}
                    </IconContainer>
                    <DisplayNameContainer>
                        <Tooltip title={displayName} showArrow={false} open={showNameTooltip ? undefined : false}>
                            <span className="test-mini-preview-class">{displayName}</span>
                        </Tooltip>
                        {columnName && (
                            <Tooltip title={columnName} showArrow={false} open={showNameTooltip ? undefined : false}>
                                <StyledDivider type="vertical" />
                                <ColumnName>{columnName}</ColumnName>
                            </Tooltip>
                        )}
                    </DisplayNameContainer>
                </TitleContainer>
            </EntityTag>
        </StyledLink>
    );
};
