import React from 'react';
import { Popover, Tag } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { Constraint as ConstraintType } from '../../../types.generated';

type Props = {
    constraint?: ConstraintType;
};

const ConstraintPopoverContent = styled.div`
    max-width: 300px;
`;

const GLOSSARY_BROWSE_PATH_ROOT = '/browse/glossary/';

export default function Constraint({ constraint }: Props) {
    const content = (
        <ConstraintPopoverContent>
            <p>{constraint?.description}</p>
            Please attach a term from the{' '}
            <Link to={`${GLOSSARY_BROWSE_PATH_ROOT}${constraint?.params?.hasGlossaryTermInNodeParams?.nodeName}`}>
                {constraint?.params?.hasGlossaryTermInNodeParams?.nodeName}
            </Link>{' '}
            node.
        </ConstraintPopoverContent>
    );

    return (
        <Popover content={content} title={constraint?.displayName}>
            <Link to={`${GLOSSARY_BROWSE_PATH_ROOT}${constraint?.params?.hasGlossaryTermInNodeParams?.nodeName}`}>
                <Tag color="#f50">Missing: {constraint?.params?.hasGlossaryTermInNodeParams?.nodeName}</Tag>
            </Link>
        </Popover>
    );
}
