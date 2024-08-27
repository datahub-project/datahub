import React from 'react';
import { Divider, Image, Tag } from 'antd';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { Maybe } from 'graphql/jsutils/Maybe';
import { ANTD_GRAY } from '../../../entity/shared/constants';

const EntityTag = styled(Tag)`
    margin: 4px;
`;

const TitleContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 4px;
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
`;

const ColumnName = styled.span`
    font-family: 'Roboto Mono', monospace;
    font-weight: bold;
`;

export const StyledDivider = styled(Divider)`
    background-color: ${ANTD_GRAY[6]};
    margin: 0 7px;
`;

type Props = {
    displayName: string;
    url: string;
    platformLogoUrl?: string;
    platformLogoUrls?: Maybe<string>[];
    logoComponent?: React.ReactNode;
    onClick?: () => void;
    columnName?: string;
};

export const EntityPreviewTag = ({
    displayName,
    url,
    platformLogoUrl,
    platformLogoUrls,
    logoComponent,
    onClick,
    columnName,
}: Props) => {
    return (
        <Link to={url} onClick={onClick}>
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
                        <span className="test-mini-preview-class">{displayName}</span>
                        {columnName && (
                            <>
                                <StyledDivider type="vertical" />
                                <ColumnName>{columnName}</ColumnName>
                            </>
                        )}
                    </DisplayNameContainer>
                </TitleContainer>
            </EntityTag>
        </Link>
    );
};
