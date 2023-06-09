import { Tooltip } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { ViewBuilderMode } from '../../entity/view/builder/types';
import { ViewBuilder } from '../../entity/view/builder/ViewBuilder';
import { buildInitialViewState, fromUnionType } from '../../entity/view/builder/utils';
import { FacetFilterInput } from '../../../types.generated';
import { UnionType } from '../utils/constants';
import { TextButton } from './styledComponents';

const ToolTipHeader = styled.div`
    margin-bottom: 12px;
`;

interface Props {
    activeFilters: FacetFilterInput[];
    unionType: UnionType;
}

export default function SaveViewButton({ activeFilters, unionType }: Props) {
    const [isViewModalVisible, setIsViewModalVisible] = useState(false);

    return (
        <>
            <Tooltip
                placement="right"
                title={
                    <>
                        <ToolTipHeader>Save these filters as a new View.</ToolTipHeader>
                        <div>Views allow you to easily save or share search filters.</div>
                    </>
                }
            >
                <TextButton
                    type="text"
                    onClick={() => setIsViewModalVisible(true)}
                    marginTop={0}
                    data-testid="save-as-view"
                >
                    Save as a View
                </TextButton>
            </Tooltip>
            {isViewModalVisible && (
                <ViewBuilder
                    mode={ViewBuilderMode.EDITOR}
                    initialState={buildInitialViewState(activeFilters, fromUnionType(unionType))}
                    onSubmit={() => setIsViewModalVisible(false)}
                    onCancel={() => setIsViewModalVisible(false)}
                />
            )}
        </>
    );
}
