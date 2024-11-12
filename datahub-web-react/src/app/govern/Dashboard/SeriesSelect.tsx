import React from 'react';

import { Button } from 'antd';
import { Tooltip } from '@components';
import { SeriesContainer, SeriesLabel, SeriesButtons } from './components';
import { ANTD_GRAY } from '../../entity/shared/constants';

import { useFormAnalyticsContext } from './FormAnalyticsContext';

export const SeriesSelect = () => {
    const {
        timeSeries: { options, setSeries, selectedSeries },
    } = useFormAnalyticsContext();
    const isActive = (key: number) => selectedSeries === key;

    return (
        <SeriesContainer>
            <SeriesLabel>Filter by Assigned Date</SeriesLabel>
            <SeriesButtons>
                {options.map((s) => (
                    <Tooltip title={s.tooltip} key={s.key} showArrow={false} placement="top">
                        <Button
                            size="small"
                            onClick={() => setSeries(s.key)}
                            style={{
                                background: isActive(s.key) ? '#11ADA0' : 'white',
                                borderColor: isActive(s.key) ? '#11ADA0' : ANTD_GRAY[6],
                                color: isActive(s.key) ? 'white' : 'inherit',
                            }}
                        >
                            {s.label}
                        </Button>
                    </Tooltip>
                ))}
            </SeriesButtons>
        </SeriesContainer>
    );
};
