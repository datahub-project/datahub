import { Image, Typography } from 'antd';
import React, { ReactNode } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { GlobalTags, Owner, GlossaryTerms, SearchInsight } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import AvatarsGroup from '../shared/avatar/AvatarsGroup';
import TagTermGroup from '../shared/tags/TagTermGroup';
import { ANTD_GRAY } from '../entity/shared/constants';
import NoMarkdownViewer from '../entity/shared/components/styled/StripMarkdownText';

interface Props {
    name: string;
    logoUrl?: string;
    logoComponent?: JSX.Element;
    url: string;
    description?: string;
    type?: string;
    platform?: string;
    qualifier?: string | null;
    tags?: GlobalTags;
    owners?: Array<Owner> | null;
    snippet?: React.ReactNode;
    insights?: Array<SearchInsight> | null;
    glossaryTerms?: GlossaryTerms;
    dataTestID?: string;
    titleSizePx?: number;
    onClick?: () => void;
}

const PreviewContainer = styled.div`
    display: flex;
    width: 100%;
    justify-content: space-between;
    align-items: center;
`;

const PlatformInfo = styled.div`
    margin-bottom: 8px;
    display: flex;
    align-items: center;
    height: 24px;
`;

const TitleContainer = styled.div`
    margin-bottom: 8px;
`;

const PreviewImage = styled(Image)`
    max-height: 18px;
    width: auto;
    object-fit: contain;
    margin-right: 10px;
    background-color: transparent;
`;

const EntityTitle = styled(Typography.Text)<{ $titleSizePx?: number }>`
    &&& {
        margin-bottom: 0;
        font-size: ${(props) => props.$titleSizePx || 16}px;
        font-weight: 600;
        vertical-align: middle;
    }
`;

const PlatformText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 700;
    color: ${ANTD_GRAY[7]};
`;

const PlatformDivider = styled.div`
    display: inline-block;
    padding-left: 10px;
    margin-right: 10px;
    border-right: 1px solid ${ANTD_GRAY[4]};
    height: 21px;
    vertical-align: text-top;
`;

const DescriptionContainer = styled.div`
    margin-top: 5px;
    color: ${ANTD_GRAY[7]};
`;

const AvatarContainer = styled.div`
    margin-top: 12px;
    margin-right: 32px;
`;

const TagContainer = styled.div`
    display: inline-block;
    margin-left: 8px;
    margin-top: -2px;
`;

const InsightContainer = styled.div`
    margin-top: 12px;
`;

const InsightsText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 600;
    color: ${ANTD_GRAY[7]};
`;

const InsightIconContainer = styled.span`
    margin-right: 4px;
`;

export default function DefaultPreviewCard({
    name,
    logoUrl,
    logoComponent,
    url,
    description,
    type,
    platform,
    // TODO(Gabe): support qualifier in the new preview card
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    qualifier,
    tags,
    owners,
    snippet,
    insights,
    glossaryTerms,
    titleSizePx,
    dataTestID,
    onClick,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const insightViews: Array<ReactNode> = [
        ...(insights?.map((insight) => (
            <>
                <InsightIconContainer>{insight.icon}</InsightIconContainer>
                <InsightsText>{insight.text}</InsightsText>
            </>
        )) || []),
    ];
    if (snippet) {
        insightViews.push(snippet);
    }
    return (
        <PreviewContainer data-testid={dataTestID}>
            <div>
                <TitleContainer>
                    <Link to={url}>
                        <PlatformInfo>
                            {(logoUrl && <PreviewImage preview={false} src={logoUrl} alt={platform || ''} />) ||
                                logoComponent}
                            {platform && <PlatformText>{platform}</PlatformText>}
                            <PlatformDivider />
                            <PlatformText>{type}</PlatformText>
                        </PlatformInfo>
                        <EntityTitle onClick={onClick} $titleSizePx={titleSizePx}>
                            {name || ' '}
                        </EntityTitle>
                    </Link>
                    <TagContainer>
                        <TagTermGroup uneditableGlossaryTerms={glossaryTerms} uneditableTags={tags} maxShow={3} />
                    </TagContainer>
                </TitleContainer>
                {description && description.length > 0 && (
                    <DescriptionContainer>
                        <NoMarkdownViewer limit={200}>{description}</NoMarkdownViewer>
                    </DescriptionContainer>
                )}
                {owners && owners.length > 0 && (
                    <AvatarContainer>
                        <AvatarsGroup size={28} owners={owners} entityRegistry={entityRegistry} maxCount={4} />
                    </AvatarContainer>
                )}
                {insightViews.length > 0 && (
                    <InsightContainer>
                        {insightViews.map((insightView, index) => (
                            <span>
                                {insightView}
                                {index < insightViews.length - 1 && <PlatformDivider />}
                            </span>
                        ))}
                    </InsightContainer>
                )}
            </div>
        </PreviewContainer>
    );
}
