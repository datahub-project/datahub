import React from 'react';
import { Route, Switch } from 'react-router-dom';
import { WizardPageMock } from '../WizardPage/WizardPage.mock';

export const MockRoutes: React.FC = () => {
  return (
    <Switch>
      <Route path="/glossaryV2/import/mock" component={WizardPageMock} />
    </Switch>
  );
};
