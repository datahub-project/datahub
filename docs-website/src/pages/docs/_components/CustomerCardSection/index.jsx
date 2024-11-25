import React from "react";
import CustomerCard from '../CustomerCard'

const customerCardContent = [
  {
    customer: "Netflix",
    imgUrl: "/img/assets/netflix.jpg",
    title: "How they are contributing to DataHub to make it more extensible",
    description: (
      <>
        <i>
          "DataHub gave us the extensibility features we needed to define new
          entity types easily and augment existing ones.
          DataHub performed exceptionally well in managing our traffic load and data
          volume. It offers <b> a great developer experience, a well-documented
          taxonomy, and — very importantly — solid community support.</b>"
        <br />
        <br />
        — Ajoy Majumdar, Software Architect at Netflix
        </i>
        <br />
      </>
    ),
    to: "https://youtu.be/ejcO7hdX0lk?si=8iPjrPeBZq5KNdb-",
  },
  {
    customer: "Visa",
    imgUrl: "/img/assets/travel.jpg",
    title: "How to use DataHub to scale your Data Governance",
    description: (
      <>
        <i>
          "We found DataHub to provide excellent coverage for our needs. What we
          appreciate most about DataHub is <b>its powerful API platform.</b>"
        <br />
        <br />
        — Jean-Pierre Dijcks, Sr. Dir. Product Management at VISA
        </i>
        <br />
      </>
    ),
    to: "https://youtu.be/B6CplqnIkFw?si=jrrr04cV5rdxO6Ra",
  },
  {
    customer: "MediaMarkt Saturn",
    imgUrl: "/img/assets/business.jpg",
    title: "Building Data Access Management within DataHub",
    description: (
      <>
        Europe’s #1 consumer electronics retailer implemented DataHub for three reasons:
        <br />
        <br />
        1. DataHub provides an extremely <b>flexible and customizable metadata platform at scale </b>
        <br />
        2. Open-source means lower cost to implement and removes the headache of license management
        <br />
        3. Community-driven project which continually evolves with industry trends and best practices
      </>
    ),
    to: "https://youtu.be/wsCFnElN_Wo?si=i-bNAQAsbHJq5O9-",
  },
  {
    customer: "Airtel",
    imgUrl: "/img/assets/phonecall.jpg",
    title: "DataHub is the Bedrock of Data Mesh at Airtel",
    description: (
      <>
        Airtel is a leading global telecommunication provider. DataHub is the
        bedrock of Data Mesh at Airtel by providing the requisite governance and
        metadata management functionality to <b>ensure their Data Products should
        are discoverable, addressable, trustworthy, self-describing, and secure.</b>
        <br />
        <br />
        Get a closer look at how the Airtel team has successfully integrated
        DataHub to take their data mesh implementation to the next level.
      </>
    ),
    to: "https://www.youtube.com/watch?v=yr24mM91BN4",
  },
];

const CustomerCardSection = () => {
  return (
    <div style={{ padding: "2vh 0" }}>
      <div className="container" style={{ padding: "0" }}>
        <div className="row row--no-gutters">
            {customerCardContent.map((props, idx) => (
                <CustomerCard key={idx} {...props} />
              ))}
        </div>
      </div>
    </div>
  );
};

export default CustomerCardSection;