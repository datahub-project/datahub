import { Typography } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { PreviewType } from '@app/entity/Entity';
import { usePreviewData } from '@app/entity/shared/PreviewContext';
import { ExpandedActorGroup } from '@app/entity/shared/components/styled/ExpandedActorGroup';
import NoMarkdownViewer from '@app/entity/shared/components/styled/StripMarkdownText';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import EntityCount from '@app/entity/shared/containers/profile/header/EntityCount';
import PlatformContentView from '@app/entity/shared/containers/profile/header/PlatformContent/PlatformContentView';
import StructuredPropertyBadge from '@app/entity/shared/containers/profile/header/StructuredPropertyBadge';
import EntityPaths from '@app/preview/EntityPaths/EntityPaths';
import { getUniqueOwners } from '@app/preview/utils';
import SearchTextHighlighter from '@app/search/matches/SearchTextHighlighter';
import TagTermGroup from '@app/shared/tags/TagTermGroup';
import useContentTruncation from '@app/shared/useContentTruncation';

import {
    Container,
    Domain,
    EntityPath,
    ErModelRelationshipCardinality,
    GlobalTags,
    GlossaryTerms,
    Maybe,
    Owner,
} from '@types';

const PreviewContainer = styled.div`
    display: flex;
    width: 100%;
    justify-content: space-between;
    align-items: center;
`;

const LeftColumn = styled.div<{ expandWidth: boolean }>`
    max-width: ${(props) => (props.expandWidth ? '100%' : '60%')};
`;

const RightColumn = styled.div`
    max-width: 40%;
    display: flex;
`;

const TitleContainer = styled.div`
    margin-bottom: 5px;
    line-height: 30px;

    .entityCount {
        margin-bottom: 2px;
    }
`;

const EntityTitleContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const EntityTitle = styled(Typography.Text)<{ $titleSizePx?: number }>`
    display: block;
    &&&:hover {
        text-decoration: underline;
    }

    &&& {
        font-size: ${(props) => props.$titleSizePx || 16}px;
        font-weight: 600;
        vertical-align: middle;
    }
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
    color: ${ANTD_GRAY[7]};
    margin-bottom: 8px;
`;

const TagContainer = styled.div`
    display: inline-flex;
    margin-left: 0px;
    margin-top: 3px;
    flex-wrap: wrap;
    margin-right: 8px;
`;

const TagSeparator = styled.div`
    margin: 2px 8px 0 0;
    height: 17px;
    border-right: 1px solid #cccccc;
`;

const UserListContainer = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: right;
    margin-right: 8px;
`;

const UserListTitle = styled(Typography.Text)`
    text-align: right;
    margin-bottom: 10px;
    padding-right: 12px;
`;

interface Props {
    name: string;
    urn: string;
    logoComponent?: JSX.Element;
    url: string;
    description?: string;
    type?: string;
    typeIcon?: JSX.Element;
    tags?: GlobalTags;
    owners?: Array<Owner> | null;
    glossaryTerms?: GlossaryTerms;
    container?: Container;
    domain?: Domain | undefined | null;
    entityCount?: number;
    displayAssetCount?: boolean;
    dataTestID?: string;
    titleSizePx?: number;
    onClick?: () => void;
    previewType?: Maybe<PreviewType>;
    paths?: EntityPath[];
    cardinality?: ErModelRelationshipCardinality | null;
}

export default function ERModelSidebarPreviewCard({
    name,
    urn,
    logoComponent,
    url,
    description,
    type,
    typeIcon,
    tags,
    owners,
    glossaryTerms,
    domain,
    container,
    entityCount,
    displayAssetCount,
    titleSizePx,
    dataTestID,
    onClick,
    previewType,
    paths,
    cardinality,
}: Props) {
    const previewData = usePreviewData();
    const hasGlossaryTerms = !!glossaryTerms?.terms?.length;
    const hasTags = !!tags?.tags?.length;
    const [descriptionExpanded, setDescriptionExpanded] = useState(false);

    const { contentRef, isContentTruncated } = useContentTruncation(container);

    const onPreventMouseDown = (event) => {
        event.preventDefault();
        event.stopPropagation();
    };

    const shouldShowRightColumn = owners && owners.length > 0;
    const uniqueOwners = getUniqueOwners(owners);

    return (
        <PreviewContainer data-testid={dataTestID} onMouseDown={onPreventMouseDown}>
            <LeftColumn key="left-column" expandWidth={!shouldShowRightColumn}>
                <TitleContainer>
                    <PlatformContentView
                        entityLogoComponent={logoComponent}
                        typeIcon={typeIcon}
                        entityType={type}
                        parentContainersRef={contentRef}
                        areContainersTruncated={isContentTruncated}
                    />
                    <EntityTitleContainer>
                        <Link to={url}>
                            (
                            <EntityTitle onClick={onClick} $titleSizePx={titleSizePx}>
                                <SearchTextHighlighter field="name" text={name || ''} />
                            </EntityTitle>
                            )
                        </Link>
                        <StructuredPropertyBadge structuredProperties={previewData?.structuredProperties} />
                    </EntityTitleContainer>
                    {entityCount && <PlatformDivider />}
                    <EntityCount entityCount={entityCount} displayAssetsText={displayAssetCount} />
                </TitleContainer>
                {paths && paths.length > 0 && <EntityPaths paths={paths} resultEntityUrn={urn || ''} />}
                {description && description.length > 0 && (
                    <DescriptionContainer>
                        <NoMarkdownViewer
                            limit={descriptionExpanded ? undefined : 250}
                            shouldWrap={previewType === PreviewType.HOVER_CARD}
                            readMore={
                                previewType === PreviewType.HOVER_CARD ? (
                                    <Typography.Link
                                        onClickCapture={(e) => {
                                            onPreventMouseDown(e);
                                            setDescriptionExpanded(!descriptionExpanded);
                                        }}
                                    >
                                        {descriptionExpanded ? 'Show Less' : 'Show More'}
                                    </Typography.Link>
                                ) : undefined
                            }
                            customRender={(text) => <SearchTextHighlighter field="description" text={text} />}
                        >
                            {description}
                        </NoMarkdownViewer>
                    </DescriptionContainer>
                )}
                {cardinality && (
                    <DescriptionContainer>
                        <span>
                            <b>Cardinality: </b>
                            {cardinality}
                        </span>
                    </DescriptionContainer>
                )}
                {(hasGlossaryTerms || hasTags) && (
                    <TagContainer>
                        {domain && <TagTermGroup domain={domain} maxShow={3} />}
                        {domain && hasGlossaryTerms && <TagSeparator />}
                        {hasGlossaryTerms && <TagTermGroup uneditableGlossaryTerms={glossaryTerms} maxShow={3} />}
                        {((hasGlossaryTerms && hasTags) || (domain && hasTags)) && <TagSeparator />}
                        {hasTags && <TagTermGroup uneditableTags={tags} maxShow={3} />}
                    </TagContainer>
                )}
            </LeftColumn>
            {shouldShowRightColumn && (
                <RightColumn key="right-column">
                    {uniqueOwners && uniqueOwners?.length > 0 && (
                        <UserListContainer>
                            <UserListTitle strong>Owners</UserListTitle>
                            <div>
                                <ExpandedActorGroup actors={uniqueOwners.map((owner) => owner.owner)} max={2} />
                            </div>
                        </UserListContainer>
                    )}
                </RightColumn>
            )}
        </PreviewContainer>
    );
}
