-- 1. Анализ текущего состояния

-- Кол-во строк в таблицах:
SELECT count(*) FROM dm.loan_holiday_info l;
10040

SELECT count(*) FROM rd.deal_info; 
10000

SELECT count(*) FROM rd.loan_holiday l ;
10000

SELECT count(*) FROM rd.product;
10000

--Группировка по effective_from_date
-- Витрина
SELECT effective_from_date, COUNT(*) AS cnt
FROM dm.loan_holiday_info
GROUP BY effective_from_date;
2023-08-11	3502
2023-03-15	3500
2023-01-01	3000

-- Сделки
SELECT effective_from_date, COUNT(*) AS cnt
FROM rd.deal_info
GROUP BY effective_from_date;
2023-08-11	3500
2023-01-01	3000

-- Каникулы
SELECT effective_from_date, COUNT(*) AS cnt
FROM rd.loan_holiday
GROUP BY effective_from_date;
2023-08-11	3500
2023-03-15	3500
2023-01-01	3000

-- Продукты
SELECT effective_from_date, COUNT(*) AS cnt
FROM rd.product
GROUP BY effective_from_date;
2023-03-15	3500
--===>
-- Выявленные проблемы:
--•	В rd.deal_info отсутствуют строки на дату 2023-03-15.
--•	В rd.product отсутствуют строки на даты 2023-01-01 и 2023-08-11.
---------------------------------------------

--2. Загрузка недостающих данных


--Данные из rd.tmp_deal_info:
SELECT count(*) FROM rd.tmp_deal_info; 
3500



SELECT effective_from_date, COUNT(*) AS cnt
FROM rd.tmp_deal_info
GROUP BY effective_from_date;
2023-03-15	3500


INSERT INTO rd.deal_info (
    deal_rk,
    deal_num,
    deal_name,
    deal_sum,
    client_rk,
    account_rk,
    agreement_rk,
    deal_start_date,
    department_rk,
    product_rk,
    deal_type_cd,
    effective_from_date,
    effective_to_date
)
SELECT 
    deal_rk,
    deal_num,
    deal_name,
    deal_sum,
    client_rk,
    account_rk,
    agreement_rk,
    deal_start_date,
    department_rk,
    product_rk,
    deal_type_cd,
    effective_from_date,
    effective_to_date
FROM rd.tmp_deal_info
WHERE effective_from_date = DATE '2023-03-15';


SELECT effective_from_date, COUNT(*) AS cnt
FROM rd.deal_info
GROUP BY effective_from_date;
2023-08-11	3500
2023-03-15	3500
2023-01-01	3000



SELECT count(*) FROM rd.tmp_product;
10000

SELECT effective_from_date, COUNT(*) AS cnt
FROM rd.tmp_product
GROUP BY effective_from_date;
2023-08-11	3500
2023-03-15	3500
2023-01-01	3000

INSERT INTO rd.product (
    product_rk,
    product_name,
    effective_from_date,
    effective_to_date
)
SELECT
    product_rk,
    product_name,
    effective_from_date,
    effective_to_date
FROM rd.tmp_product
WHERE effective_from_date IN ('2023-01-01', '2023-08-11');


SELECT effective_from_date, COUNT(*) AS cnt
FROM rd.product
GROUP BY effective_from_date;
2023-08-11	3500
2023-03-15	3500
2023-01-01	3000

--3. Перегрузка витрины

CREATE OR REPLACE PROCEDURE dm.fill_loan_holiday_info()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE dm.loan_holiday_info;
    INSERT INTO dm.loan_holiday_info (
        deal_rk,
        effective_from_date,
        effective_to_date,
        agreement_rk,
        client_rk,
        department_rk,
        product_rk,
        product_name,
        deal_type_cd,
        deal_start_date,
        deal_name,
        deal_number,
        deal_sum,
        loan_holiday_type_cd,
        loan_holiday_start_date,
        loan_holiday_finish_date,
        loan_holiday_fact_finish_date,
        loan_holiday_finish_flg,
        loan_holiday_last_possible_date
    )
    SELECT
        d.deal_rk,
        lh.effective_from_date,
        lh.effective_to_date,
        d.agreement_rk,
        d.client_rk,
        d.department_rk,
        d.product_rk,
        p.product_name,
        d.deal_type_cd,
        d.deal_start_date,
        d.deal_name,
        d.deal_num,
        d.deal_sum,
        lh.loan_holiday_type_cd,
        lh.loan_holiday_start_date,
        lh.loan_holiday_finish_date,
        lh.loan_holiday_fact_finish_date,
        lh.loan_holiday_finish_flg,
        lh.loan_holiday_last_possible_date
    FROM rd.deal_info d
    LEFT JOIN rd.loan_holiday lh
        ON d.deal_rk = lh.deal_rk
       AND d.effective_from_date = lh.effective_from_date
    LEFT JOIN rd.product p
        ON p.product_rk = d.product_rk
       AND p.effective_from_date = d.effective_from_date;

    RAISE NOTICE 'Загрузка в dm.loan_holiday_info завершена.';
END;
$$;

CALL dm.fill_loan_holiday_info();

--4. Проверка результатов
SELECT effective_from_date, COUNT(*) AS cnt
FROM dm.loan_holiday_info
GROUP BY effective_from_date;
2023-08-11	3522
2023-03-15	3510
2023-01-01	3008


SELECT *
FROM dm.loan_holiday_info
GROUP BY
    deal_rk,
    effective_from_date,
    effective_to_date,
    agreement_rk,
    account_rk,
    client_rk,
    department_rk,
    product_rk,
    product_name,
    deal_type_cd,
    deal_start_date,
    deal_name,
    deal_number,
    deal_sum,
    loan_holiday_type_cd,
    loan_holiday_start_date,
    loan_holiday_finish_date,
    loan_holiday_fact_finish_date,
    loan_holiday_finish_flg,
    loan_holiday_last_possible_date
HAVING COUNT(*) > 1;



