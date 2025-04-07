import React from 'react';
import styled from 'styled-components';
import { Space, Table, Typography } from 'antd';
import moment from 'moment';
import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { InfoItem } from '../../shared/components/styled/InfoItem';
import { GetMlModelDeploymentQuery } from '../../../../graphql/mlModelDeployment.generated';

const TabContent = styled.div`
    padding: 16px;
`;

const InfoItemContainer = styled.div<{ justifyContent }>`
    display: flex;
    position: relative;
    justify-content: ${(props) => props.justifyContent};
    padding: 0px 2px;
`;

const InfoItemContent = styled.div`
    padding-top: 8px;
    width: 100px;
    overflow-wrap: break-word;
`;

export default function MLModelDeploymentSummary() {
    const baseEntity = useBaseEntity<GetMlModelDeploymentQuery>();
    const mmd = baseEntity?.mlModelDeployment;

    return (
        <TabContent>
            <Space direction="vertical" style={{ width: '100%' }} size="large">
                <Typography.Title level={3}>Details</Typography.Title>
                <InfoItemContainer justifyContent="left">
                    <InfoItem title="Created At">
                        <InfoItemContent>
                            {/* {mmd?.properties?.createdAt
                                ? moment(mmd.properties.createdAt).format('YYYY-MM-DD HH:mm:ss')
                                : '-'} */}
                        </InfoItemContent>
                    </InfoItem>
                </InfoItemContainer>
            </Space>
        </TabContent>
    );
}
