import { Text, Tooltip, colors } from '@components';
import { Divider, Image, Tag } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import ProposedIcon from '@src/app/entityV2/shared/sidebarSection/ProposedIcon';
import { useEmbeddedProfileLinkProps } from '@src/app/shared/useEmbeddedProfileLinkProps';

const EntityTag = styled(Tag)<{ $isProposed?: boolean }>`
    margin: 4px;
    max-width: inherit;
    border-color: ${colors.gray[200]} !important;

    ${(props) =>
        props.$isProposed &&
        `
            border: 1px dashed ${colors.gray[200]};
        `}
`;

const TitleContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 2px 4px;
    max-width: inherit;
`;

const IconContainer = styled.span`
    padding-left: 3px;
    padding-right: 3px;
    display: flex;
    align-items: center;
`;

const PlatformLogo = styled(Image)`
    max-height: 12px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const DisplayNameContainer = styled.span`
    padding-left: 2px;
    padding-right: 2px;
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

const ProposedIconContainer = styled.div`
    display: flex;
    align-items: center;
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
    isProposed?: boolean;
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
    isProposed,
}: Props) => {
    const externalUrl = constructExternalUrl(url);
    const linkProps = useEmbeddedProfileLinkProps();

    const renderTag = () => {
        return (
            <EntityTag $isProposed={isProposed}>
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
                            <Text
                                type="span"
                                color="gray"
                                size="sm"
                                lineHeight="sm"
                                className="test-mini-preview-class"
                            >
                                {displayName}
                            </Text>
                        </Tooltip>
                        {columnName && (
                            <Tooltip title={columnName} showArrow={false} open={showNameTooltip ? undefined : false}>
                                <StyledDivider type="vertical" />
                                <ColumnName>{columnName}</ColumnName>
                            </Tooltip>
                        )}
                    </DisplayNameContainer>
                    <ProposedIconContainer>
                        {isProposed && <ProposedIcon propertyName="Entity" />}
                    </ProposedIconContainer>
                </TitleContainer>
            </EntityTag>
        );
    };

    return (
        <>
            {isProposed ? (
                <>{renderTag()}</>
            ) : (
                <StyledLink
                    to={externalUrl}
                    {...linkProps}
                    onClick={!isProposed ? onClick : undefined}
                    data-testid={dataTestId}
                >
                    {renderTag()}
                </StyledLink>
            )}
        </>
    );
};
