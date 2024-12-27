import { CalendarData } from '@src/alchemy-components/components/CalendarChart/types';
import { getColorAccessor } from '@src/alchemy-components/components/CalendarChart/utils';
import { useMemo } from 'react';
import { OperationType } from '@src/types.generated';
import { ValueType } from './useChangeHistoryData';

const DEFAULT_COLOR = '#EBECF0';
const INSERTS_OR_UPDATES_COLORS = ['#CAC3F1', '#705EE4', '#3E2F9D'];
const DELETES_COLORS = ['#F2998D', '#BF4636', '#A32C1C'];

export default function useColorAccessors(data: CalendarData<ValueType>[], types: OperationType[]) {
    return useMemo(() => {
        const maxInsertsAndUpdates = Math.max(
            ...data.map((datum) => (datum.value.inserts || 0) + (datum.value.updates || 0)),
            0,
        );
        const maxDeletes = Math.max(...data.map((datum) => datum.value.deletes || 0), 0);

        const colorAccessor = getColorAccessor<ValueType>(
            data,
            {
                insertsAndUpdates: {
                    valueAccessor: (datum) =>
                        (types.includes(OperationType.Insert) ? datum.inserts : 0) +
                        (types.includes(OperationType.Update) ? datum.updates : 0),
                    colors: INSERTS_OR_UPDATES_COLORS,
                },
                deletes: {
                    valueAccessor: (datum) => (types.includes(OperationType.Delete) ? datum.deletes : 0),
                    colors: DELETES_COLORS,
                },
            },
            DEFAULT_COLOR,
        );

        const insertsColorAccessor = getColorAccessor<ValueType>(
            data,
            {
                inserts: {
                    valueAccessor: (datum) => datum.inserts,
                    colors: INSERTS_OR_UPDATES_COLORS,
                },
            },
            INSERTS_OR_UPDATES_COLORS[0],
            maxInsertsAndUpdates,
        );

        const updatesColorAccessor = getColorAccessor<ValueType>(
            data,
            {
                updates: {
                    valueAccessor: (datum) => datum.updates,
                    colors: INSERTS_OR_UPDATES_COLORS,
                },
            },
            INSERTS_OR_UPDATES_COLORS[0],
            maxInsertsAndUpdates,
        );

        const deletesColorAccessor = getColorAccessor<ValueType>(
            data,
            {
                deletes: {
                    valueAccessor: (datum) => datum.deletes,
                    colors: DELETES_COLORS,
                },
            },
            DELETES_COLORS[0],
            maxDeletes,
        );

        return {
            day: colorAccessor,
            inserts: insertsColorAccessor,
            updates: updatesColorAccessor,
            deletes: deletesColorAccessor,
        };
    }, [data, types]);
}
