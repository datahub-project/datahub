import React from 'react';
import styled from 'styled-components';
import { Assertion } from '../../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../../constants';
import { AcrylDatasetAssertionsList } from '../../AcrylAssertionsList';
import { DataContractCategoryType } from './types';

const Category = styled.div`
    padding: 20px;
    font-weight: bold;
    font-size: 14px;
    background-color: ${ANTD_GRAY[3]};
    border-radius: 4px;
`;

const Hint = styled.span`
    font-weight: normal;
    font-size: 14px;
    color: ${ANTD_GRAY[8]};
`;

type Props = {
    category: DataContractCategoryType;
    multiple?: boolean;
    assertions: Assertion[];
    selectedUrns: string[];
    onSelect: (assertionUrn: string) => void;
};

/**
 * Used for selecting the assertions that are part of a data contract
 */
export const DataContractAssertionGroupSelect = ({
    category,
    assertions,
    multiple = true,
    selectedUrns,
    onSelect,
}: Props) => {
    return (
        <>
            <Category>
                {category} <Hint> {!multiple && `(Choose 1)`}</Hint>
            </Category>
            <AcrylDatasetAssertionsList
                assertions={assertions}
                showMenu={false}
                showSelect
                selectedUrns={selectedUrns}
                onSelect={onSelect}
            />
        </>
    );
};
