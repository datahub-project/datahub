import React from "react";
import CustomerCard from '../CustomerCard'
import {
  EyeTwoTone,
  HeartTwoTone,
  ApiTwoTone,
  AlertTwoTone,
  CompassTwoTone,
  ProfileTwoTone,
} from "@ant-design/icons";

const customerCardContent =  [
{
    customer: "Netflix",
    title: "How they are contributing to DataHub to make it more extensible",
    description: ` <i>DataHub gave us the extensibility features we needed to define new entity types easily and augment existing ones.
                    During our evaluation, we assessed both functional and nonfunctional aspects, and DataHub performed exceptionally well in managing our traffic load and data volume.
                    It offers a great developer experience, a well-documented taxonomy, and — very importantly — solid community support.”</i>
                    Ajoy Majumdar, Software Architect at Netflix.`,
    to: "https://youtu.be/ejcO7hdX0lk?si=8iPjrPeBZq5KNdb-",
  },
{
    customer: "VISA",
    title: "How to use DataHub to scale your Data Governance",
    description: "<quote>We found DataHub to provide excellent coverage for our needs. What we appreciate most about DataHub is its powerful API platform.” </quote> - Jean-Pierre Dijcks, Sr. Dir. Product Management at VISA",
    to: "https://youtu.be/B6CplqnIkFw?si=jrrr04cV5rdxO6Ra",
  },
{
    customer: "MediaMarkt Saturn",
    title: "Building Data Access Management within DataHub",
    description: `MediaMarkt Saturn implemented DataHub for three reasons:

1. DataHub provides an extremely flexible and customizable metadata platform at scale
2. Open-source means lower cost to implement and removes the headache of license management
3. Community-driven project which continually evolves with industry trends and best practices

Hear about their adoption journey and get a demo of their full-featured, customized workflow to create and manage data access requests within DataHub.`,
    to: "https://www.acryldata.io/blog/data-contracts-in-datahub-combining-verifiability-with-holistic-data-management?utm_source=datahub&utm_medium=referral&utm_content=blog",
  },
{
  customer: "Airtel",
  title: "DataHub is the Bedrock of Data Mesh at Airtel",
  description: `Airtel is a leading global telecommunication provider.
  DataHub is the bedrock of Data Mesh at Airtel by providing the requisite governance and metadata management functionality to ensure their Data Products should are discoverable, addressable, trustworthy, self-describing, and secure.
  Get a closer look at how the Airtel team has successfully integrated DataHub to take their data mesh implementation to the next level.`,
  to: "https://youtu.be/wsCFnElN_Wo?si=i-bNAQAsbHJq5O9-",
  icon: <ProfileTwoTone />
}
]

const customerCards = () => {
return (
    <div style={{ padding: "2vh 0" }}>
      <div className="container" style={{ padding: "0"}}>
        <div className="row row--no-gutters">
          {customerCardContent.map((props, idx) => (
            <CustomerCard key={idx} {...props} />
          ))}
        </div>
      </div>
    </div>
  );
};

export default customerCards;
