CREATE TABLE public.customers (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    city        TEXT,
    signed_up   TIMESTAMP
);

CREATE TABLE public.orders (
    id           INTEGER PRIMARY KEY,
    customer_id  INTEGER REFERENCES public.customers (id),
    status       TEXT,
    amount       NUMERIC(10, 2),
    created_at   TIMESTAMP
);

INSERT INTO public.customers (id, name, city, signed_up) VALUES
    (1, 'Ada Lovelace',   'London',   '2023-01-15 09:00:00'),
    (2, 'Alan Turing',    'Cambridge','2023-02-20 11:30:00'),
    (3, 'Grace Hopper',   'New York', '2023-03-05 14:15:00');

INSERT INTO public.orders (id, customer_id, status, amount, created_at) VALUES
    (1, 1, 'completed',  120.50, '2023-04-01 10:00:00'),
    (2, 1, 'shipped',     75.00, '2023-04-03 16:45:00'),
    (3, 2, 'completed',  200.00, '2023-04-10 12:30:00'),
    (4, 3, 'processing',  49.99, '2023-04-12 09:20:00');
