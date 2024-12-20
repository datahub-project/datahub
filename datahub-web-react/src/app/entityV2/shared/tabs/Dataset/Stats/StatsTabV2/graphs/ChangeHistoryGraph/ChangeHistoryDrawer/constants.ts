import { capitalizeFirstLetter } from '@src/app/shared/textUtil';
import { OperationType } from '@src/types.generated';

export const AVAILABLE_OPERATION_TYPES = [OperationType.Insert, OperationType.Update, OperationType.Delete];

export const DEFAULT_OPERATION_TYPES = AVAILABLE_OPERATION_TYPES;

export const OPERATION_TYPE_OPTIONS = AVAILABLE_OPERATION_TYPES.map((operationType) => ({
    value: operationType.toString(),
    label: capitalizeFirstLetter(operationType) as string,
}));

export const OPERATIONS_LIMIT = 100;
