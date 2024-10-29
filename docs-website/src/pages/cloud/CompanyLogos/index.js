import React, { useEffect, useRef } from 'react';
import Link from '@docusaurus/Link';
import styles from './logos.module.scss';
import customersData from './customersData.json';
import clsx from 'clsx';

const ScrollingCustomers = ({ noOverlay = true, spacing, ...rest }) => {
  const customers = customersData.customers;
  const duplicatedLogos = [...customers, ...customers, ...customers];
  const scrollingRef = useRef(null);

  useEffect(() => {
    if (scrollingRef.current) {
      scrollingRef.current.style.setProperty('--customers-length', customers.length);
    }
  }, [customers.length]);

  return (
    <div
      className={clsx(styles.scrollingCustomers, {
        [styles.noOverlay]: noOverlay,
      })}
      {...rest}
    >
      <div
        className={clsx(styles.animateScrollingCustomers, styles.scrollingCustomers__inner)}
        ref={scrollingRef}
      >
        {duplicatedLogos.map((customer, index) => (
          <Link
            key={`item-${index}`}
            to={customer.link.href}
            target={customer.link.blank ? '_blank' : '_self'}
            rel={customer.link.blank ? 'noopener noreferrer' : ''}
            style={{ minWidth: 'max-content', padding: '0 1.8rem' }}
          >
            <img
              src={customer.logo.asset._ref}
              alt={customer.logo.alt}
              className={styles.logoItem}
            />
          </Link>
        ))}
      </div>
    </div>
  );
};

export default ScrollingCustomers;
