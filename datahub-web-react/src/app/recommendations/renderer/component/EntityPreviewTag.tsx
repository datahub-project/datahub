import { Tooltip } from '@components';
import { useEmbeddedProfileLinkProps } from '@src/app/shared/useEmbeddedProfileLinkProps';
import { Divider, Image, Tag } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../entity/shared/constants';

const EntityTag = styled(Tag)`
    margin: 4px;
    max-width: inherit;
`;

const TitleContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 4px;
    max-width: inherit;
`;

const IconContainer = styled.span`
    padding-left: 4px;
    padding-right: 4px;
    display: flex;
    align-items: center;
`;

const PlatformLogo = styled(Image)`
    max-height: 16px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const DisplayNameContainer = styled.span`
    padding-left: 4px;
    padding-right: 4px;
    overflow: hidden;
    display: flex;
    align-items: center;
    span {
        overflow: hidden;
        text-overflow: ellipsis;
    }
`;

const ColumnName = styled.span`
    font-family: 'Roboto Mono', monospace;
    font-weight: bold;
`;

export const StyledDivider = styled(Divider)`
    background-color: ${ANTD_GRAY[6]};
    margin: 0 7px;
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
}: Props) => {
    const externalUrl = constructExternalUrl(url);
    const linkProps = useEmbeddedProfileLinkProps();

    return (
        <StyledLink to={externalUrl} {...linkProps} onClick={onClick} data-testid={dataTestId}>
            <EntityTag>
                <TitleContainer>
                    <IconContainer>
                        {(!!platformLogoUrl && !platformLogoUrls && (
                            <PlatformLogo preview={false} src={platformLogoUrl} alt="none" />
                        )) ||
                            (!!platformLogoUrls &&
                                platformLogoUrls.slice(0, 2).map((platformLogoUrlsEntry) => (
                                    <>
                                        <PlatformLogo preview={false} src={platformLogoUrlsEntry || ''} alt="none" />
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
