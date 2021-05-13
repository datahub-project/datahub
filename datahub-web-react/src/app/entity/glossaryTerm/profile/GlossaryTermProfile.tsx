import { Alert, Col, Divider, Row } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useGetGlossaryTermQuery } from '../../../../graphql/glossaryTerm.generated';
import { EntityType } from '../../../../types.generated';
import { useGetEntitySearchResults } from '../../../../utils/customGraphQL/useGetEntitySearchResults';
import useUserParams from '../../../shared/entitySearch/routingUtils/useUserParams';
import { Message } from '../../../shared/Message';
import { useEntityRegistry } from '../../../useEntityRegistry';
import GlossaryTermDetails from './GlossaryTermDetails';
import GlossaryTermHeader from './GlossaryTermHeader';

const PageContainer = styled.div`
    padding: 32px 100px;
`;
const messageStyle = { marginTop: '10%' };

export default function GlossaryTermProfile() {
    const { urn, subview, item } = useUserParams();
    const { loading, error, data } = useGetGlossaryTermQuery({ variables: { urn } });

    const entityRegistry = useEntityRegistry();
    const searchTypes = entityRegistry.getSearchEntityTypes();
    searchTypes.splice(searchTypes.indexOf(EntityType.GlossaryTerm), 1);

    const glossaryTermName = data?.glossaryTerm?.name;
    const ownershipResult = useGetEntitySearchResults(
        {
            query: `${glossaryTermName}`,
        },
        searchTypes,
    );

    const contentLoading =
        Object.keys(ownershipResult).some((type) => {
            return ownershipResult[type].loading;
        }) || loading;

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    Object.keys(ownershipResult).forEach((type) => {
        const entities = ownershipResult[type].data?.search?.entities;
        if (!entities || entities.length === 0) {
            delete ownershipResult[type];
        } else {
            ownershipResult[type] = ownershipResult[type].data?.search?.entities;
        }
    });

    return (
        <PageContainer>
            {contentLoading && <Message type="loading" content="Loading..." style={messageStyle} />}
            <Row style={{ padding: '20px 0px 10px 0px' }}>
                <Col span={24}>
                    <h1>{data?.glossaryTerm?.name}</h1>
                </Col>
            </Row>
            <GlossaryTermHeader
                definition={data?.glossaryTerm?.glossaryTermInfo?.definition || ''}
                sourceRef={data?.glossaryTerm?.glossaryTermInfo?.sourceRef || ''}
                sourceUrl={data?.glossaryTerm?.glossaryTermInfo?.sourceUrl as string}
                customProperties={
                    (data?.glossaryTerm?.glossaryTermInfo.customProperties as Array<{ key: string; value: string }>) ||
                    []
                }
                ownership={data?.glossaryTerm?.ownership}
            />
            <Divider />
            <GlossaryTermDetails urn={urn} subview={subview} item={item} ownerships={ownershipResult} />
        </PageContainer>
    );
}
