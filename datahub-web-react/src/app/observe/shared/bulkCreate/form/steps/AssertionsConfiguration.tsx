import { Text, colors } from '@components';
import { Info } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

const MoreAssertionsWrapper = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 4px;
`;

const FormsWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

type Props = {
    freshnessForm: React.ReactNode;
    volumeForm: React.ReactNode;
};

export const AssertionsConfiguration = ({ freshnessForm, volumeForm }: Props) => {
    return (
        <div>
            <FormsWrapper>
                {freshnessForm}

                {volumeForm}
            </FormsWrapper>

            <br />

            {/** *** More Assertion Types **** */}
            <MoreAssertionsWrapper>
                <Info color={colors.gray[400]} size={16} />
                <Text color="gray" colorLevel={400} weight="medium">
                    More assertion types available on individual dataset pages.
                </Text>
            </MoreAssertionsWrapper>
        </div>
    );
};
