import { Text, colors } from '@components';
import { Alert } from 'antd';
import { Info } from 'phosphor-react';
import React from 'react';

type Props = {
    freshnessForm: React.ReactNode;
    volumeForm: React.ReactNode;
};

export const AssertionsConfiguration = ({ freshnessForm, volumeForm }: Props) => {
    return (
        <div>
            {freshnessForm}
            <br />

            {volumeForm}
            <br />

            {/** *** More Assertion Types **** */}
            <Alert
                message={
                    <div>
                        <Text size="md" color="gray" colorLevel={600} weight="semiBold">
                            More assertion types
                        </Text>
                        <Text size="sm" color="gray" colorLevel={600}>
                            For all available assertion types, use the &apos;Quality&apos; tab on individual dataset
                            pages.
                        </Text>
                    </div>
                }
                type="warning"
                icon={<Info color={colors.yellow[600]} size={16} />}
                style={{ marginTop: 16, borderRadius: 12, borderColor: colors.yellow[200] }}
                showIcon
            />
        </div>
    );
};
