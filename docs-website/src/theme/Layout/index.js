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
    </>
  );
}