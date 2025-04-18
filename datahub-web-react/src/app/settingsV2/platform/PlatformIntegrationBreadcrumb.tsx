import React from 'react';
import { Breadcrumb } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

type Props = {
    name: string;
};

const BreadcrumbContainer = styled.div`
    margin-bottom: 14px;
`;

export const PlatformIntegrationBreadcrumb = ({ name }: Props) => {
    return (
        <BreadcrumbContainer>
            <Breadcrumb>
                <Breadcrumb.Item>
                    <Link to="/settings/integrations">Integrations</Link>
                </Breadcrumb.Item>
                <Breadcrumb.Item>{name}</Breadcrumb.Item>
            </Breadcrumb>
        </BreadcrumbContainer>
    );
};
