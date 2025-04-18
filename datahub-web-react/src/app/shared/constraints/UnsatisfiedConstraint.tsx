import { Popover } from '@components';
import { Tag } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { Constraint as ConstraintType } from '@types';

type Props = {
    constraint?: ConstraintType;
};

const UnsatisfiedConstraintPopoverContent = styled.div`
    max-width: 300px;
`;

const GLOSSARY_BROWSE_PATH_ROOT = '/browse/glossary/';

export default function UnsatisfiedConstraint({ constraint }: Props) {
    const content = (
        <UnsatisfiedConstraintPopoverContent>
            <p>{constraint?.description}</p>
            Please attach a term from the{' '}
            <Link to={`${GLOSSARY_BROWSE_PATH_ROOT}${constraint?.params?.hasGlossaryTermInNodeParams?.nodeName}`}>
                {constraint?.params?.hasGlossaryTermInNodeParams?.nodeName}
            </Link>{' '}
            node.
        </UnsatisfiedConstraintPopoverContent>
    );

    return (
        <Popover content={content} title={constraint?.displayName}>
            <Link to={`${GLOSSARY_BROWSE_PATH_ROOT}${constraint?.params?.hasGlossaryTermInNodeParams?.nodeName}`}>
                <Tag color="#f50">Missing: {constraint?.params?.hasGlossaryTermInNodeParams?.nodeName}</Tag>
            </Link>
        </Popover>
    );
}
