import React from 'react';
import { Divider, Typography } from 'antd';
import styled from 'styled-components';
import { useListPostsQuery } from '../../graphql/post.generated';
import { Post, PostContentType } from '../../types.generated';
import { PostTextCard } from '../search/PostTextCard';
import { PostLinkCard } from '../search/PostLinkCard';

const RecommendationContainer = styled.div`
    margin-bottom: 32px;
    max-width: 1000px;
    min-width: 750px;
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

const LinkPostsContainer = styled.div`
    display: flex;
    flex-direction: row;
`;

export const HomePagePosts = () => {
    const { data: postsData } = useListPostsQuery({
        variables: {
            input: {
                start: 0,
                count: 10,
            },
        },
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
            {textPosts.map((post) => (
                <PostTextCard textPost={post as Post} />
            ))}
            <LinkPostsContainer>
                {linkPosts.map((post, index) => (
                    <PostLinkCard linkPost={post as Post} index={index} />
                ))}
            </LinkPostsContainer>
        </RecommendationContainer>
    ) : (
        <></>
    );
};
