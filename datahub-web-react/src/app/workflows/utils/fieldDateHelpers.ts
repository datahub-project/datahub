import moment from 'moment';

import { FieldValue } from '@app/workflows/hooks/useWorkflowFormCompletion';

/**
 * Converts a FieldValue to a moment date object for DatePicker component
 * Matches original logic exactly: if instanceof Date use moment(value), else if truthy use moment(Number(value))
 */
export function convertFieldValueToMoment(value: FieldValue): moment.Moment | null {
    if (value instanceof Date) {
        return moment(value);
    }
    if (value) {
        return moment(Number(value));
    }

    return null;
}

/**
 * Converts a DatePicker value back to a Date for field values
 * Handles the DatePickerValue type which can be moment.Moment | null | undefined
 */
export function convertDatePickerToFieldValue(date: moment.Moment | null | undefined): FieldValue {
    return date ? date.toDate() : null;
}

/**
 * @deprecated Use convertDatePickerToFieldValue instead
 * Converts a moment date object back to a Date for field values
 */
export function convertMomentToFieldValue(date: moment.Moment | null): FieldValue {
    return date ? date.toDate() : null;
}
