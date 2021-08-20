import { QueryResult } from '@apollo/client';
import { Divider, List, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { GetGlossaryTermQuery, useGetGlossaryTermQuery } from '../../../../graphql/glossaryTerm.generated';
import { EntityType, Exact } from '../../../../types.generated';
import { Message } from '../../../shared/Message';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

export type Props = {
    glossaryRelatedTermType: string;
    glossaryRelatedTermResult: Array<any>;
};

const ListContainer = styled.div`
    display: default;
    flex-grow: default;
`;

const TitleContainer = styled.div`
    margin-bottom: 30px;
`;

const ListItem = styled.div`
    margin: 40px;
    padding-bottom: 5px;
`;

const Profile = styled.div`
    marging-bottom: 20px;
`;

const messageStyle = { marginTop: '10%' };

export default function GlossaryRelatedTermsResult({ glossaryRelatedTermType, glossaryRelatedTermResult }: Props) {
    const entityRegistry = useEntityRegistry();
    const glossaryRelatedTermUrns: Array<string> = [];
    glossaryRelatedTermResult.forEach((item: any) => {
        glossaryRelatedTermUrns.push(item?.entity?.urn);
    });
    const glossaryTermInfo: QueryResult<GetGlossaryTermQuery, Exact<{ urn: string }>>[] = [];

    for (let i = 0; i < glossaryRelatedTermUrns.length; i++) {
        glossaryTermInfo.push(
            // eslint-disable-next-line react-hooks/rules-of-hooks
            useGetGlossaryTermQuery({
                variables: {
                    urn: glossaryRelatedTermUrns[i],
                },
            }),
        );
    }

    const contentLoading = glossaryTermInfo.some((item) => {
        return item.loading;
    });
    return (
        <>
            {contentLoading ? (
                <Message type="loading" content="Loading..." style={messageStyle} />
            ) : (
                <ListContainer>
                    <TitleContainer>
                        <Typography.Title level={3}>{glossaryRelatedTermType}</Typography.Title>
                        <Divider />
                    </TitleContainer>
                    <List
                        dataSource={glossaryTermInfo}
                        renderItem={(item) => {
                            return (
                                <ListItem>
                                    <Profile>
                                        {entityRegistry.renderPreview(
                                            EntityType.GlossaryTerm,
                                            PreviewType.PREVIEW,
                                            item?.data?.glossaryTerm,
                                        )}
                                    </Profile>
                                    <Divider />
                                </ListItem>
                            );
                        }}
                    />
                </ListContainer>
            )}
        </>
    );
}
