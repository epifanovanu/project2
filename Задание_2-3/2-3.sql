----Задание 2.3

--1. Проверка и корректировка account_in_sum (если некорректен account_in_sum, используем account_out_sum предыдущего дня)
SELECT 
    curr.account_rk,
    curr.effective_date,
    curr.account_in_sum AS wrong_in_sum,
    prev.account_out_sum AS correct_in_sum
FROM rd.account_balance curr
JOIN rd.account_balance prev
    ON curr.account_rk = prev.account_rk
   AND curr.effective_date = prev.effective_date + INTERVAL '1 day'
WHERE curr.account_in_sum IS DISTINCT FROM prev.account_out_sum;

--2. Проверка и корректировка account_out_sum (если некорректен account_out_sum, используем account_in_sum следующего дня)
SELECT 
    prev.account_rk,
    prev.effective_date,
    prev.account_out_sum AS wrong_out_sum,
    curr.account_in_sum AS correct_out_sum
FROM rd.account_balance prev
JOIN rd.account_balance curr
    ON curr.account_rk = prev.account_rk
   AND curr.effective_date = prev.effective_date + INTERVAL '1 day'
WHERE prev.account_out_sum IS DISTINCT FROM curr.account_in_sum;

--3. Обновление данных в rd.account_balance 
--3.1 корректировка account_in_sum (по данным предыдущего дня)
UPDATE rd.account_balance curr
SET account_in_sum = prev.account_out_sum
FROM rd.account_balance prev
WHERE curr.account_rk = prev.account_rk
  AND curr.effective_date = prev.effective_date + INTERVAL '1 day'
  AND curr.account_in_sum IS DISTINCT FROM prev.account_out_sum;

--3.2 корректировка account_out_sum (по данным следующего дня)
UPDATE rd.account_balance curr
SET account_out_sum = next.account_in_sum
FROM rd.account_balance next
WHERE curr.account_rk = next.account_rk
  AND curr.effective_date = next.effective_date - INTERVAL '1 day'
  AND curr.account_out_sum IS DISTINCT FROM next.account_in_sum;

--4. Процедура загрузки витрины 
CREATE OR REPLACE PROCEDURE dm.fill_account_balance_turnover()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE dm.account_balance_turnover;

    INSERT INTO dm.account_balance_turnover (
        account_rk,
        currency_name,
        department_rk,
        effective_date,
        account_in_sum,
        account_out_sum
    )
    SELECT 
        ab.account_rk,
        dc.currency_name,
        a.department_rk,
        ab.effective_date,
        ab.account_in_sum,
        ab.account_out_sum
    FROM rd.account_balance ab
    JOIN rd.account a 
        ON ab.account_rk = a.account_rk 
       AND ab.effective_date BETWEEN a.effective_from_date AND a.effective_to_date
    JOIN dm.dict_currency dc 
        ON a.currency_cd = dc.currency_cd
       AND ab.effective_date BETWEEN dc.effective_from_date AND dc.effective_to_date;

    RAISE NOTICE 'Загрузка витрины dm.account_balance_turnover завершена.';
END;
$$;

CALL dm.fill_account_balance_turnover();

--5. Проверка после обновлений 

--5.1 Проверка account_in_sum
SELECT 
    curr.account_rk,
    curr.effective_date,
    curr.account_in_sum AS wrong_in_sum,
    prev.account_out_sum AS correct_in_sum
FROM dm.fill_account_balance_turnover curr
JOIN dm.fill_account_balance_turnover prev
    ON curr.account_rk = prev.account_rk
   AND curr.effective_date = prev.effective_date + INTERVAL '1 day'
WHERE curr.account_in_sum IS DISTINCT FROM prev.account_out_sum
ORDER BY curr.account_rk, curr.effective_date;

--5.2 Проверка account_out_sum 

SELECT  
prev.account_rk, 
prev.effective_date, 
prev.account_out_sum AS wrong_out_sum, 
curr.account_in_sum AS correct_out_sum 
FROM rd.account_balance prev 
JOIN rd.account_balance curr 
ON curr.account_rk = prev.account_rk 
AND curr.effective_date = prev.effective_date + INTERVAL '1 day' 
WHERE prev.account_out_sum IS DISTINCT FROM curr.account_in_sum;

