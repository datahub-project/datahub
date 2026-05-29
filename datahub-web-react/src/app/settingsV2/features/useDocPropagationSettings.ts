import { message } from 'antd';
import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useGetDocPropagationSettingsQuery, useUpdateDocPropagationSettingsMutation } from '@graphql/app.generated';

// Hook to get the document propagation settings & manage state
export const useGetDocPropagationSettings = () => {
    const { data, refetch } = useGetDocPropagationSettingsQuery();
    const [isColPropagateChecked, setIsColPropagateChecked] = useState<boolean>(false);

    useEffect(() => {
        const docPropSetting = data?.docPropagationSettings?.docColumnPropagation;
        if (docPropSetting !== undefined) setIsColPropagateChecked(!!docPropSetting);
    }, [data]);

    return {
        isColPropagateChecked,
        setIsColPropagateChecked,
        refetch,
    };
};

// Hook to update the document propagation settings
export const useUpdateDocPropagationSettings = () => {
    const { t } = useTranslation('settings.features');
    const [updateDocPropagationSettings] = useUpdateDocPropagationSettingsMutation();
    const { refetch } = useGetDocPropagationSettingsQuery();

    const updateDocPropagation = async (checked: boolean) => {
        try {
            await updateDocPropagationSettings({
                variables: {
                    input: {
                        docColumnPropagation: checked,
                    },
                },
            });
            refetch();
            message.success(t('docPropagation.updateSuccess'));
        } catch (e) {
            message.error(t('docPropagation.updateError'));
            refetch();
        }
    };

    return { updateDocPropagation };
};
