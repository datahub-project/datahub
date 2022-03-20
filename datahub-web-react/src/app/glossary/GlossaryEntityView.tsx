import React from 'react';
import { Col, Row } from 'antd';
import { useParams } from 'react-router-dom';
import { SearchablePage } from '../search/SearchablePage';
import { GlossariesTreeSidebar } from './GlossariesTreeSidebar';
import { EntityType } from '../../types.generated';
import { EntityProfileNavBar } from '../entity/shared/containers/profile/nav/EntityProfileNavBar';

export const GlossaryEntityView = () => {
    const { urn: encodedUrn } = useParams<{ urn: string }>();
    const entityType = EntityType.GlossaryTerm;

    return (
        <SearchablePage>
            <Row>
                <Col
                    span={4}
                    style={{
                        borderRight: '1px solid #E9E9E9',
                        padding: 40,
                    }}
                >
                    <GlossariesTreeSidebar />
                </Col>
                <Col span={20}>
                    <EntityProfileNavBar urn={encodedUrn} entityType={entityType} />
                </Col>
            </Row>
        </SearchablePage>
    );
};
