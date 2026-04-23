-- TPC-C Clause 3.3 consistency check queries, one per condition,
-- in condition-ID order (1st statement = condition 1, etc.).
-- Parsed by jTPCCConsistencyCheck using the same ;-split convention
-- as the other sql.common scripts (\; is the literal-semicolon escape,
-- not needed by any current query).

-- Condition 1: for each warehouse W, W_YTD = sum(D_YTD).
SELECT w.w_id, w.w_ytd, d.sum_d_ytd
  FROM bmsql_warehouse w
  JOIN (SELECT d_w_id, SUM(d_ytd) AS sum_d_ytd
          FROM bmsql_district
         GROUP BY d_w_id) d
    ON w.w_id = d.d_w_id
 WHERE w.w_ytd <> d.sum_d_ytd
 LIMIT 1;

-- Condition 2: for each district,
--   D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID).
-- max(NO_O_ID) may be NULL (all pending new orders delivered).
SELECT d.d_w_id, d.d_id, d.d_next_o_id,
       o_agg.max_o_id, no_agg.max_no_o_id
  FROM bmsql_district d
  LEFT JOIN (SELECT o_w_id, o_d_id, MAX(o_id) AS max_o_id
               FROM bmsql_oorder
              GROUP BY o_w_id, o_d_id) o_agg
    ON d.d_w_id = o_agg.o_w_id AND d.d_id = o_agg.o_d_id
  LEFT JOIN (SELECT no_w_id, no_d_id, MAX(no_o_id) AS max_no_o_id
               FROM bmsql_new_order
              GROUP BY no_w_id, no_d_id) no_agg
    ON d.d_w_id = no_agg.no_w_id AND d.d_id = no_agg.no_d_id
 WHERE o_agg.max_o_id IS NULL
    OR (d.d_next_o_id - 1) <> o_agg.max_o_id
    OR (no_agg.max_no_o_id IS NOT NULL
        AND (d.d_next_o_id - 1) <> no_agg.max_no_o_id)
 LIMIT 1;

-- Condition 3: for each district, NEW_ORDER NO_O_IDs are contiguous.
SELECT no_w_id, no_d_id,
       MIN(no_o_id) AS min_o,
       MAX(no_o_id) AS max_o,
       COUNT(*)     AS cnt
  FROM bmsql_new_order
 GROUP BY no_w_id, no_d_id
HAVING MAX(no_o_id) - MIN(no_o_id) + 1 <> COUNT(*)
 LIMIT 1;

-- Condition 4: for each district, sum(O_OL_CNT) = count(ORDER_LINE).
SELECT a.w_id, a.d_id, a.sum_ol_cnt, b.count_ol
  FROM (SELECT o_w_id AS w_id, o_d_id AS d_id,
               SUM(o_ol_cnt) AS sum_ol_cnt
          FROM bmsql_oorder
         GROUP BY o_w_id, o_d_id) a
  JOIN (SELECT ol_w_id AS w_id, ol_d_id AS d_id,
               COUNT(*) AS count_ol
          FROM bmsql_order_line
         GROUP BY ol_w_id, ol_d_id) b
    ON a.w_id = b.w_id AND a.d_id = b.d_id
 WHERE a.sum_ol_cnt <> b.count_ol
 LIMIT 1;

-- Condition 5: for each order, O_CARRIER_ID IS NULL iff a matching
-- NEW_ORDER row exists.
SELECT o.o_w_id, o.o_d_id, o.o_id,
       CASE WHEN o.o_carrier_id IS NULL THEN 1 ELSE 0 END AS carrier_null,
       CASE WHEN no.no_o_id IS NULL THEN 0 ELSE 1 END AS has_new_order
  FROM bmsql_oorder o
  LEFT JOIN bmsql_new_order no
    ON o.o_w_id = no.no_w_id
   AND o.o_d_id = no.no_d_id
   AND o.o_id   = no.no_o_id
 WHERE (o.o_carrier_id IS NULL) <> (no.no_o_id IS NOT NULL)
 LIMIT 1;

-- Condition 6: for each order, O_OL_CNT = count(matching ORDER_LINE).
SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_ol_cnt,
       COALESCE(ol_c.cnt, 0) AS ol_cnt
  FROM bmsql_oorder o
  LEFT JOIN (SELECT ol_w_id, ol_d_id, ol_o_id,
                    COUNT(*) AS cnt
               FROM bmsql_order_line
              GROUP BY ol_w_id, ol_d_id, ol_o_id) ol_c
    ON o.o_w_id = ol_c.ol_w_id
   AND o.o_d_id = ol_c.ol_d_id
   AND o.o_id   = ol_c.ol_o_id
 WHERE o.o_ol_cnt <> COALESCE(ol_c.cnt, 0)
 LIMIT 1;

-- Condition 7: for each order line, OL_DELIVERY_D IS NULL iff the
-- parent O_CARRIER_ID IS NULL.
SELECT ol.ol_w_id, ol.ol_d_id, ol.ol_o_id, ol.ol_number,
       CASE WHEN ol.ol_delivery_d IS NULL THEN 1 ELSE 0 END AS ol_null,
       CASE WHEN o.o_carrier_id IS NULL THEN 1 ELSE 0 END AS carrier_null
  FROM bmsql_order_line ol
  JOIN bmsql_oorder o
    ON ol.ol_w_id = o.o_w_id
   AND ol.ol_d_id = o.o_d_id
   AND ol.ol_o_id = o.o_id
 WHERE (ol.ol_delivery_d IS NULL) <> (o.o_carrier_id IS NULL)
 LIMIT 1;

-- Condition 8: for each warehouse, W_YTD = sum(H_AMOUNT).
SELECT w.w_id, w.w_ytd, h.sum_h_amount
  FROM bmsql_warehouse w
  JOIN (SELECT h_w_id, SUM(h_amount) AS sum_h_amount
          FROM bmsql_history
         GROUP BY h_w_id) h
    ON w.w_id = h.h_w_id
 WHERE w.w_ytd <> h.sum_h_amount
 LIMIT 1;

-- Condition 9: for each district, D_YTD = sum(H_AMOUNT).
SELECT d.d_w_id, d.d_id, d.d_ytd, h.sum_h_amount
  FROM bmsql_district d
  JOIN (SELECT h_w_id, h_d_id, SUM(h_amount) AS sum_h_amount
          FROM bmsql_history
         GROUP BY h_w_id, h_d_id) h
    ON d.d_w_id = h.h_w_id AND d.d_id = h.h_d_id
 WHERE d.d_ytd <> h.sum_h_amount
 LIMIT 1;

-- Condition 10: for each customer,
--   C_BALANCE = sum(delivered OL_AMOUNT) - sum(H_AMOUNT).
SELECT c.c_w_id, c.c_d_id, c.c_id, c.c_balance,
       COALESCE(ol_agg.sum_ol, 0) AS sum_delivered,
       COALESCE(h_agg.sum_h,  0) AS sum_paid
  FROM bmsql_customer c
  LEFT JOIN (SELECT o.o_w_id, o.o_d_id, o.o_c_id,
                    SUM(ol.ol_amount) AS sum_ol
               FROM bmsql_order_line ol
               JOIN bmsql_oorder o
                 ON ol.ol_w_id = o.o_w_id
                AND ol.ol_d_id = o.o_d_id
                AND ol.ol_o_id = o.o_id
              WHERE ol.ol_delivery_d IS NOT NULL
              GROUP BY o.o_w_id, o.o_d_id, o.o_c_id) ol_agg
    ON c.c_w_id = ol_agg.o_w_id
   AND c.c_d_id = ol_agg.o_d_id
   AND c.c_id   = ol_agg.o_c_id
  LEFT JOIN (SELECT h_c_w_id, h_c_d_id, h_c_id,
                    SUM(h_amount) AS sum_h
               FROM bmsql_history
              GROUP BY h_c_w_id, h_c_d_id, h_c_id) h_agg
    ON c.c_w_id = h_agg.h_c_w_id
   AND c.c_d_id = h_agg.h_c_d_id
   AND c.c_id   = h_agg.h_c_id
 WHERE c.c_balance <>
       (COALESCE(ol_agg.sum_ol, 0) - COALESCE(h_agg.sum_h, 0))
 LIMIT 1;
