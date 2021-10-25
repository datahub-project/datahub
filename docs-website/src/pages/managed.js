import React from "react";
import Layout from "@theme/Layout";
import HubspotForm from "../components/HubspotForm";

function Managed() {
  return (
    <Layout title="Managed DataHub">
      <div className="container">
        <div
          className="row"
          style={{ padding: "5vh 2rem", justifyContent: "center" }}
        >
          <div className="col col--6">
            <HubspotForm formId="ae8c7cd1-1db3-4483-b1db-b169813b9129" />
          </div>
        </div>
      </div>
    </Layout>
  );
}

export default Managed;
