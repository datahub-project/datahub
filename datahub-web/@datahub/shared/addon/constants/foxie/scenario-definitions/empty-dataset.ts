import { IFoxieScenario } from '@datahub/shared/types/foxie/service';
import { UserFunctionType } from '@datahub/shared/constants/foxie/user-function-type';
import { IUserFunctionObject } from '@datahub/shared/types/foxie/user-function-object';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import FoxieDynamicComponentsHeader from '@datahub/shared/components/foxie/dynamic-components/header';

export const emptyDatasetFoxieScenario: IFoxieScenario = {
  triggers: [
    {
      computeFromObject(ufo: IUserFunctionObject): boolean {
        return Boolean(
          ufo.functionType === UserFunctionType.ApiResponse &&
            ufo.functionTarget === 'search' &&
            ufo.functionContext?.includes(DatasetEntity.displayName) &&
            ufo.functionContext?.includes('empty')
        );
      }
    }
  ],
  actionParameters: {
    init: {
      name: 'foxie/dynamic-components/container',
      options: {
        components: [
          {
            name: 'foxie/dynamic-components/header',
            options: {
              text: `Hey ${FoxieDynamicComponentsHeader.headerNamePlaceholder}, can't find what you're looking for?`
            }
          },
          {
            name: 'dynamic-components/text',
            options: { text: 'A dataset may not appear in DataHub if it has been created less than 24 hours ago' }
          },
          { name: 'dynamic-components/text', options: { text: 'You can also try: ' } },
          {
            name: 'foxie/dynamic-components/link-button',
            options: {
              text: 'View recommended datasets',
              linkTo: 'user.profile.tab',
              linkModels: ['me', 'userdymilist-datasets']
            }
          }
        ]
      }
    }
  }
};
