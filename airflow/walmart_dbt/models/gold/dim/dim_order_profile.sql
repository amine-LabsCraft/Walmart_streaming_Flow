-- Junk dimension: every combination of payment method (hence channel) and
-- order status. Keeps descriptive flags out of the fact table.
WITH payments AS (
    SELECT * FROM VALUES
        (1, 'Debit Card', 'In-store', 'Carte de débit'),
        (2, 'Credit Card', 'In-store', 'Carte de crédit'),
        (3, 'Cash', 'In-store', 'Comptant'),
        (4, 'Gift Card', 'In-store', 'Carte-cadeau'),
        (5, 'Online', 'Online', 'Paiement en ligne')
    AS t(payment_rank, payment_method, order_channel, payment_label)
),

statuses AS (
    SELECT * FROM VALUES
        (1, 'Completed', 'Payée', TRUE),
        (2, 'Pending', 'En préparation', FALSE),
        (3, 'Cancelled', 'Annulée', FALSE),
        (4, 'Returned', 'Retournée', FALSE)
    AS t(status_rank, order_status, status_label, is_net_sale)
)

SELECT
    payment_rank * 10 + status_rank AS order_profile_key,
    order_channel,
    CASE order_channel WHEN 'Online' THEN 'En ligne' ELSE 'En magasin' END AS channel_label,
    payment_method,
    payment_label,
    order_status,
    status_label,
    is_net_sale
FROM payments
CROSS JOIN statuses
