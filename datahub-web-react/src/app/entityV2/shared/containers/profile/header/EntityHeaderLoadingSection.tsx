import { Skeleton } from '@components';
import * as React from 'react';
import styled from 'styled-components';

const Wrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    min-height: 50px;
    width: 100%;
`;

const NamesColumn = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const NameSkeleton = styled(Skeleton.Input)`
    border-radius: 4px;
    margin-right: 12px;
`;

export default function EntityTitleLoadingSection() {
    return (
        <Wrapper>
            <Skeleton.Avatar />

            <NamesColumn>
                <NameSkeleton width={240} height={20} />
                <NameSkeleton width={240} height={20} />
            </NamesColumn>
        </Wrapper>
    );
}
