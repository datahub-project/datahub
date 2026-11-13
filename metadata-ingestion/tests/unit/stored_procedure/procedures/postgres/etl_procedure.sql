INSERT INTO public.processed_orders (order_id, customer_id, total)
SELECT id AS order_id, customer_id, amount AS total FROM public.raw_orders;
