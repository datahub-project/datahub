import { DEFAULT_OPERATION_TYPES } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/constants';
import {
    AggregationGroup,
    OperationsData,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { createColorAccessors } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/utils';
import { CalendarData } from '@src/alchemy-components/components/CalendarChart/types';
import { OperationType } from '@src/types.generated';

function getSampleOfOperationsData(value: number): OperationsData {
    return {
        summary: {
            mom: null,
            totalOperations: value * 7,
            totalCustomOperations: value,
        },
        operations: {
            inserts: {
                value,
                key: 'INSERT',
                group: AggregationGroup.Purple,
                type: OperationType.Insert,
                name: 'Insert',
            },
            updates: {
                value,
                key: 'UPDATE',
                group: AggregationGroup.Purple,
                type: OperationType.Update,
                name: 'Update',
            },
            deletes: {
                value,
                key: 'DELETE',
                group: AggregationGroup.Red,
                type: OperationType.Delete,
                name: 'Delete',
            },
            alters: {
                value,
                key: 'ALTER',
                group: AggregationGroup.Purple,
                type: OperationType.Alter,
                name: 'Alter',
            },
            creates: {
                value,
                key: 'CREATE',
                group: AggregationGroup.Purple,
                type: OperationType.Create,
                name: 'Create',
            },
            drops: {
                value,
                key: 'DROP',
                group: AggregationGroup.Red,
                type: OperationType.Drop,
                name: 'Drop',
            },
            custom_custom_type_1: {
                value,
                group: AggregationGroup.Purple,
                type: OperationType.Custom,
                customType: 'custom_type_1',
                name: 'custom_type_1',
                key: 'custom_custom_type_1',
            },
        },
    };
}

const CALENDAR_DATA_SAMPLE: CalendarData<OperationsData>[] = [
    { day: '2024-01-01', value: getSampleOfOperationsData(10) },
    { day: '2024-01-02', value: getSampleOfOperationsData(20) },
    { day: '2024-01-03', value: getSampleOfOperationsData(30) },
    { day: '2024-01-04', value: getSampleOfOperationsData(40) },
    { day: '2024-01-05', value: getSampleOfOperationsData(50) },
];

describe('createColorAccessors', () => {
    it('should correctly create color accessors for day and each operation type', () => {
        const customType = 'custom_custom_type_1';

        const response = createColorAccessors(getSampleOfOperationsData(150), CALENDAR_DATA_SAMPLE, [
            ...DEFAULT_OPERATION_TYPES,
            customType,
        ]);

        expect(response.day(getSampleOfOperationsData(0))).toBe('#EBECF0');
        expect(response.day(getSampleOfOperationsData(25))).toBe('rgb(119, 103, 218)');
        expect(response.day(getSampleOfOperationsData(50))).toBe('rgb(62, 47, 157)');

        expect(response[OperationType.Update](getSampleOfOperationsData(0))).toBe('#CAC3F1');
        expect(response[OperationType.Update](getSampleOfOperationsData(25))).toBe('rgb(158, 146, 233)');
        expect(response[OperationType.Update](getSampleOfOperationsData(50))).toBe('rgb(119, 103, 218)');

        expect(response[OperationType.Insert](getSampleOfOperationsData(0))).toBe('#CAC3F1');
        expect(response[OperationType.Insert](getSampleOfOperationsData(25))).toBe('rgb(158, 146, 233)');
        expect(response[OperationType.Insert](getSampleOfOperationsData(50))).toBe('rgb(119, 103, 218)');

        expect(response[OperationType.Create](getSampleOfOperationsData(0))).toBe('#CAC3F1');
        expect(response[OperationType.Create](getSampleOfOperationsData(25))).toBe('rgb(158, 146, 233)');
        expect(response[OperationType.Create](getSampleOfOperationsData(50))).toBe('rgb(119, 103, 218)');

        expect(response[OperationType.Alter](getSampleOfOperationsData(0))).toBe('#CAC3F1');
        expect(response[OperationType.Alter](getSampleOfOperationsData(25))).toBe('rgb(158, 146, 233)');
        expect(response[OperationType.Alter](getSampleOfOperationsData(50))).toBe('rgb(119, 103, 218)');

        expect(response[customType](getSampleOfOperationsData(0))).toBe('#CAC3F1');
        expect(response[customType](getSampleOfOperationsData(25))).toBe('rgb(158, 146, 233)');
        expect(response[customType](getSampleOfOperationsData(50))).toBe('rgb(119, 103, 218)');

        expect(response[OperationType.Delete](getSampleOfOperationsData(0))).toBe('#F2998D');
        expect(response[OperationType.Delete](getSampleOfOperationsData(25))).toBe('rgb(217, 113, 99)');
        expect(response[OperationType.Delete](getSampleOfOperationsData(50))).toBe('rgb(195, 80, 64)');

        expect(response[OperationType.Drop](getSampleOfOperationsData(0))).toBe('#F2998D');
        expect(response[OperationType.Drop](getSampleOfOperationsData(25))).toBe('rgb(217, 113, 99)');
        expect(response[OperationType.Drop](getSampleOfOperationsData(50))).toBe('rgb(195, 80, 64)');
    });
});
