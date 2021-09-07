import { Image, Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { GlobalTags, Owner, GlossaryTerms } from '../../types.generated';
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
    description: string;
    type?: string;
    platform?: string;
    qualifier?: string | null;
    tags?: GlobalTags;
    owners?: Array<Owner> | null;
    snippet?: React.ReactNode;
    glossaryTerms?: GlossaryTerms;
    dataTestID?: string;
}

const PreviewContainer = styled.div`
    margin-bottom: 8px;
    display: flex;
    width: 100%;
    justify-content: space-between;
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

const EntityTitle = styled(Typography.Text)`
    &&& {
        margin-bottom: 0;
        font-size: 16px;
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
`;

const AvatarContainer = styled.div`
    margin-top: 12px;
`;

const TagContainer = styled.div`
    display: inline-block;
    margin-left: 8px;
    margin-top: -2px;
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
    glossaryTerms,
    dataTestID,
}: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <PreviewContainer data-testid={dataTestID}>
            <div>
                <Link to={url}>
                    <TitleContainer>
                        <PlatformInfo>
                            {logoComponent}
                            {!!logoUrl && <PreviewImage preview={false} src={logoUrl} alt={platform} />}
                            <PlatformText>{platform}</PlatformText>
                            <PlatformDivider />
                            <PlatformText>{type}</PlatformText>
                        </PlatformInfo>
                        <Link to={url}>
                            <EntityTitle>{name || ' '}</EntityTitle>
                        </Link>
                        <TagContainer>
                            <TagTermGroup uneditableGlossaryTerms={glossaryTerms} uneditableTags={tags} maxShow={3} />
                        </TagContainer>
                    </TitleContainer>
                </Link>
                {description.length > 0 && (
                    <DescriptionContainer>
                        <NoMarkdownViewer limit={200}>{description}</NoMarkdownViewer>
                    </DescriptionContainer>
                )}
                {snippet}
            </div>
            <AvatarContainer>
                <AvatarsGroup owners={owners} entityRegistry={entityRegistry} maxCount={4} />
            </AvatarContainer>
        </PreviewContainer>
    );
}
