import React from "react";
import QuickstartCard from '../QuickstartCard'

const quickstartContent =  [
{
    title: "Quickstart with DataHub",
    icon: "datahub-logo-color-light-horizontal",
    to: "quickstart",
    color: '#FFF',
    fontColor: '#091013',
  },
{
    title: "Learn about DataHub Cloud",
    icon: "datahub-cloud-color-dark-horizontal",
    to: "managed-datahub/managed-datahub-overview",
    color: '#e6f4ff',
    fontColor: '#091013',
}
]

const QuickstartCards = () => {
return (
    <div style={{ padding: "2vh 0" }}>
      <div className="container" style={{ padding: "0"}}>
        <div className="row row--no-gutters">
          {quickstartContent.map((props, idx) => (
            <QuickstartCard key={idx} {...props} />
          ))}
        </div>
      </div>
    </div>
  );
};

export default QuickstartCards;
