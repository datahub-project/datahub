import { IngestionSourceType, shouldIncludeSource } from '../IngestionSourceList';
import { CLI_EXECUTOR_ID } from '../utils';

describe('shouldIncludeSource', () => {
    it('should return true if the source filter is for CLI and the source is a CLI source', () => {
        const source = { config: { executorId: CLI_EXECUTOR_ID } };
        const isSourceIncluded = shouldIncludeSource(source, IngestionSourceType.CLI);
        expect(isSourceIncluded).toBe(true);
    });

    it('should return false if the source filter is for CLI and the source is not a CLI source', () => {
        const source = { config: { executorId: 'default' } };
        const isSourceIncluded = shouldIncludeSource(source, IngestionSourceType.CLI);
        expect(isSourceIncluded).toBe(false);
    });

    it('should return true if the source filter is for UI and the source is a UI source', () => {
        const source = { config: { executorId: 'default' } };
        const isSourceIncluded = shouldIncludeSource(source, IngestionSourceType.UI);
        expect(isSourceIncluded).toBe(true);
    });

    it('should return false if the source filter is for UI and the source is not a UI source', () => {
        const source = { config: { executorId: CLI_EXECUTOR_ID } };
        const isSourceIncluded = shouldIncludeSource(source, IngestionSourceType.UI);
        expect(isSourceIncluded).toBe(false);
    });

    it('should return true no matter what if the source type is all', () => {
        const source1 = { config: { executorId: CLI_EXECUTOR_ID } };
        const isSourceIncluded1 = shouldIncludeSource(source1, IngestionSourceType.ALL);
        expect(isSourceIncluded1).toBe(true);

        const source2 = { config: { executorId: 'default' } };
        const isSourceIncluded2 = shouldIncludeSource(source2, IngestionSourceType.ALL);
        expect(isSourceIncluded2).toBe(true);
    });
});
