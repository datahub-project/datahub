import { SizeOptions } from '@src/alchemy-components/theme/config';

export type JustifyContentOptions = 'center' | 'flex-start';

export type AlignItemsOptions = 'center' | 'flex-start' | 'none';

export type LoaderProps = {
    size?: SizeOptions;
    justifyContent?: JustifyContentOptions;
    alignItems?: AlignItemsOptions;
};
