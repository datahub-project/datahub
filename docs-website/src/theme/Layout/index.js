import React from 'react';
import Layout from '@theme-original/Layout';
import SecondNavbar from '../../components/SecondNavbar/SecondNavbar';

export default function LayoutWrapper(props) {
  return (
    <>
      <Layout {...props}>
        <SecondNavbar />
        {props.children}
      </Layout>
      <img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=92db07cf-8934-4b30-857a-3fcfda4c86dd" style={{ display: 'none' }} />
    </>
  );
}