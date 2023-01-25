-- WBVARDEF purchase_start_date = 2021-01-01;
-- WBVARDEF purchase_end_date = 2021-02-01;
--
-- WBVARDEF bu_a = 6; --1 OR 6
-- WBVARDEF bu_b = 16; --2 OR 16
-- WBVARDEF bu_c = 10; --3 OR 10
--
-- WBVARDEF region = eu; -- na OR eu;
-- WBVARDEF database_name = eu_zeta; --na_zeta OR eu_zeta;
-- WBVARDEF customer_info_table = sas_customer_mstr_bu_vw; --na_sas_customer_mstr_bu_vw OR sas_customer_mstr_bu_vw;
-- WBVARDEF customer_lvl_info_table = sas_customer_mstr_vw;
-- WBVARDEF transaction_table = sas_transaction_all_vw; --na_sas_transaction_all_vw OR sas_transaction_all_vw;
-- WBVARDEF interactions_table = $[region]_mi.$[region]_pdc_2022_interactions_count;
-- WBVARDEF labels_table = $[region]_mi.$[region]_pdc_2022_labels;
--
-- WBVARDEF customer_group = p_2021;
-- WBVARDEF pdc_slides_bucket = rl-sandbox/nana/kedro_tutorial/;

--------------- GET CUSTOMERS AND TRANSACTIONS OF INTEREST --------------- 
DROP TABLE IF EXISTS #customer_transactions;
CREATE TABLE #customer_transactions AS 
(
    SELECT 
        cust.mstr_customer_id,
        cust.original_purchase_date,
        cust.business_unit,
        trx.order_id,
        trx.transaction_date,
        trx.amount,
        trx.sku,
        trx.quantity,
        trx.quantity*trx.amount as spend
    FROM $[database_name].$[customer_info_table] cust
    INNER JOIN $[database_name].$[transaction_table] trx 
    ON
        cust.mstr_customer_id = trx.mstr_customer_id AND
        cust.business_unit = trx.business_unit 
    WHERE
        -- Filter for only NA business units
        (cust.business_unit = $[bu_a] OR cust.business_unit = $[bu_b] OR cust.business_unit = $[bu_c]) AND
        trx.transaction_date >= '$[purchase_start_date]' AND
        trx.transaction_date < '$[purchase_end_date]' AND
        -- Filter for orders and purchases
        trx.transaction_sub_type = 1 AND
        trx.sales_credit = 1       
);


---------------CUSTOMER SPEND AGGREGATES --------------------------------------------------------
DROP TABLE IF EXISTS #customer_spend_aggregates;
CREATE TABLE #customer_spend_aggregates AS
(
    SELECT 
        mstr_customer_id,
        business_unit,
        MIN(spend) as minimum_spend,
        MAX(spend) as maximum_spend,
        AVG(spend) as average_spend, 
        SUM(spend) as total_spend,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY spend) as median_spend,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY spend) as spend_percentile_25,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY spend) as spend_percentile_75
    FROM
        #customer_transactions
    GROUP BY mstr_customer_id, business_unit
);

-------------------------------------------------------------------------------------------------
-- SELECT * from #customer_spend_aggregates LIMIT 100;

UNLOAD ('SELECT * from #customer_spend_aggregates;')
TO 's3://$[pdc_slides_bucket]$[region]/$[customer_group]/customer_spend_aggregates.csv'
iam_role 'arn:aws:iam::773919108708:role/Redshift-Spectrum-Role' CSV header parallel off allowoverwrite;
