import React from 'react';
import { Route, Switch } from 'react-router-dom';
import { AcrylPageRoutes } from '../conf/Global';
import { IntroduceYourself } from './homeV2/IntroduceYourself';

export default function AcrylRoutes() {
    return (
        <Switch>
            <Route exact path={AcrylPageRoutes.INTRODUCE} render={() => <IntroduceYourself />} />
        </Switch>
    );
}
