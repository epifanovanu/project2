---------------
---1.	Поиск дублей:
SELECT client_rk, effective_from_date, COUNT(*)
FROM dm.client
GROUP BY client_rk, effective_from_date
HAVING COUNT(*) > 1;
---------------------

--2.	Проверка строк, подлежащих удалению:
SELECT *
FROM dm.client c
WHERE EXISTS (
    SELECT 1
    FROM dm.client sub
    WHERE sub.client_rk = c.client_rk
      AND sub.effective_from_date = c.effective_from_date
      AND ctid <> (
          SELECT MIN(ctid)
          FROM dm.client
          WHERE client_rk = sub.client_rk
            AND effective_from_date = sub.effective_from_date
      )
);
-----------------------
--3.	Удаление дубликатов:
DELETE FROM dm.client
WHERE ctid NOT IN (
    SELECT MIN(ctid)
    FROM dm.client
    GROUP BY client_rk, effective_from_date
);

----------------
--4.	Проверка результата:
SELECT client_rk, effective_from_date, COUNT(*)
FROM dm.client
GROUP BY client_rk, effective_from_date
HAVING COUNT(*) > 1;



