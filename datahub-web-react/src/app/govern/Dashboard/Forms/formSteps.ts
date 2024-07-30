import AssignAssetsStep from './AssignAssetsStep';
import AssignUsersStep from './AssignUsersStep';
import CreateQuestionsStep from './CreateQuestionsStep';
import FormDetailsStep from './FormDetailsStep';

export const formSteps = [
    {
        number: 1,
        name: 'Form Details',
        description: 'Fill In form details for reference',
        component: FormDetailsStep,
    },
    {
        number: 2,
        name: 'Create Questions',
        description: 'Questions to collect information on needed assests',
        component: CreateQuestionsStep,
    },
    {
        number: 3,
        name: 'Assign Assets',
        description: 'Assign the Assets for which you want to collect the data',
        component: AssignAssetsStep,
    },
    {
        number: 4,
        name: 'Assign Users',
        description: 'Assign Users to whom you want to collect the data from',
        component: AssignUsersStep,
    },
];
