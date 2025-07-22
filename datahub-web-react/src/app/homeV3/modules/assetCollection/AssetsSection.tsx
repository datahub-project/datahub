import { colors } from '@components';
import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import SelectAssetsSection from '@app/homeV3/modules/assetCollection/SelectAssetsSection';
import SelectedAssetsSection from '@app/homeV3/modules/assetCollection/SelectedAssetsSection';

const Container = styled.div`
    display: flex;
    width: 100%;
    gap: 16px;
`;

const LeftSection = styled.div`
    flex: 6;
`;

const RightSection = styled.div`
    flex: 4;
`;

export const VerticalDivider = styled(Divider)`
    color: ${colors.gray[100]};
    height: auto;
`;

type Props = {
    selectedAssetUrns: string[];
    setSelectedAssetUrns: React.Dispatch<React.SetStateAction<string[]>>;
};

const AssetsSection = ({ selectedAssetUrns, setSelectedAssetUrns }: Props) => {
    return (
        <Container>
            <LeftSection>
                <SelectAssetsSection
                    selectedAssetUrns={selectedAssetUrns}
                    setSelectedAssetUrns={setSelectedAssetUrns}
                />
            </LeftSection>
            <VerticalDivider type="vertical" />
            <RightSection>
                <SelectedAssetsSection
                    selectedAssetUrns={selectedAssetUrns}
                    setSelectedAssetUrns={setSelectedAssetUrns}
                />
            </RightSection>
        </Container>
    );
};

export default AssetsSection;
