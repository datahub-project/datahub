import { DatasetProfile } from '../../../../../types.generated';

export const completeSampleProfile: DatasetProfile = {
    rowCount: 1000,
    columnCount: 2000,
    timestampMillis: 0,
    fieldProfiles: [
        {
            fieldPath: 'testColumn',
            uniqueCount: 1,
            uniqueProportion: 0.111,
            nullCount: 2,
            nullProportion: 0.222,
            min: '3',
            max: '4',
            mean: '5',
            median: '6',
            stdev: '7',
            sampleValues: ['value1', 'value2', 'value3'],
        },
        {
            fieldPath: 'testColumn2',
            uniqueCount: 8,
            uniqueProportion: 0.333,
            nullCount: 9,
            nullProportion: 0.444,
            min: '10',
            max: '11',
            mean: '12',
            median: '13',
            stdev: '14',
            sampleValues: ['value4', 'value5', 'value6'],
        },
    ],
};

export const missingFieldStatsProfile: DatasetProfile = {
    rowCount: 1000,
    columnCount: 2000,
    timestampMillis: 0,
};

export const missingTableStatsProfile: DatasetProfile = {
    timestampMillis: 0,
    fieldProfiles: [
        {
            fieldPath: 'testColumn',
            uniqueCount: 1,
            uniqueProportion: 0.111,
            nullCount: 2,
            nullProportion: 0.222,
            min: '3',
            max: '4',
            mean: '5',
            median: '6',
            stdev: '7',
            sampleValues: ['value1', 'value2', 'value3'],
        },
        {
            fieldPath: 'testColumn2',
            uniqueCount: 8,
            uniqueProportion: 0.333,
            nullCount: 9,
            nullProportion: 0.444,
            min: '10',
            max: '11',
            mean: '12',
            median: '13',
            stdev: '14',
            sampleValues: ['value4', 'value5', 'value6'],
        },
    ],
};
