import { ColorOptions } from '@src/alchemy-components/theme/config';
import { capitalizeFirstLetter } from '@src/app/shared/textUtil';
import { OperationType } from '@src/types.generated';

export function getPillColorByOperationType(operationType: OperationType): ColorOptions | undefined {
    switch (operationType) {
        case OperationType.Insert:
        case OperationType.Update:
            return 'violet';
        case OperationType.Delete:
            return 'red';
        default:
            return undefined;
    }
}

export function getOperationName(operationType: OperationType): string {
    return capitalizeFirstLetter(operationType) as string;
}
