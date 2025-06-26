import { Divider, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { PostLinkCard } from '@app/search/PostLinkCard';
import { PostTextCard } from '@app/search/PostTextCard';
import { getHomePagePostsFilters } from '@app/utils/queryUtils';

import { useListPostsQuery } from '@graphql/post.generated';
import { Post, PostContentType } from '@types';

const RecommendationContainer = styled.div`
    margin-bottom: 92px;
    max-width: 1000px;
`;

const ContentContainer = styled.div`
    padding-right: 12px;
    padding-left: 12px;
`;

const RecommendationTitle = styled(Typography.Title)`
    margin-top: 0px;
    margin-bottom: 0px;
    padding: 0px;
`;

const ThinDivider = styled(Divider)`
    margin-top: 12px;
    margin-bottom: 12px;
`;

const TextPostsContainer = styled.div`
    margin-bottom: 12px;
    display: flex;
    flex-direction: column;
    gap: 18px;
`;

const LinkPostsContainer = styled.div`
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    gap: 12px;
    margin-bottom: 12px;
`;

export const HomePagePosts = () => {
    const { data: postsData } = useListPostsQuery({
        variables: {
            input: {
                start: 0,
                count: 10,
                orFilters: getHomePagePostsFilters(),
            },
        },
        fetchPolicy: 'cache-first',
    });

    const textPosts =
        postsData?.listPosts?.posts?.filter((post) => post?.content?.contentType === PostContentType.Text) || [];
    const linkPosts =
        postsData?.listPosts?.posts?.filter((post) => post?.content?.contentType === PostContentType.Link) || [];
    const hasPosts = textPosts.length > 0 || linkPosts.length > 0;
    return hasPosts ? (
        <RecommendationContainer>
            <RecommendationTitle level={4}>Pinned</RecommendationTitle>
            <ThinDivider />
            <ContentContainer>
                <TextPostsContainer>
                    {textPosts.map((post) => (
                        <PostTextCard textPost={post as Post} />
                    ))}
                </TextPostsContainer>
                <LinkPostsContainer>
                    {linkPosts.map((post) => (
                        <PostLinkCard linkPost={post as Post} />
                    ))}
                </LinkPostsContainer>
            </ContentContainer>
        </RecommendationContainer>
    ) : (
        <></>
    );
};
