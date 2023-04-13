SET search_path = dannys_diner;



CREATE TABLE members(
	customer_id varchar(1),
	join_date DATE
);
	
INSERT INTO members 
	("customer_id", "join_date") VALUES
	('A', '2021-01-07'),
	('B', '2021-01-09');
	
CREATE TABLE menu(
	"product_id" INTEGER,
	"product_name" VARCHAR(5),
	"price" INTEGER
);


INSERT INTO menu
	("product_id", "product_name", "price") VALUES
	('1', 'sushi', '10'),
  	('2', 'curry', '15'),
  	('3', 'ramen', '12');
	

CREATE TABLE sales (
	"customer_id" VARCHAR(1),
	"order_date" DATE,
	"product_id" INTEGER
	); 



INSERT INTO sales
	("customer_id", "order_date", "product_id") VALUES
	('A', '2021-01-01', '1'),
  ('A', '2021-01-01', '2'),
  ('A', '2021-01-07', '2'),
  ('A', '2021-01-10', '3'),
  ('A', '2021-01-11', '3'),
  ('A', '2021-01-11', '3'),
  ('B', '2021-01-01', '2'),
  ('B', '2021-01-02', '2'),
  ('B', '2021-01-04', '1'),
  ('B', '2021-01-11', '1'),
  ('B', '2021-01-16', '3'),
  ('B', '2021-02-01', '3'),
  ('C', '2021-01-01', '3'),
  ('C', '2021-01-01', '3'),
  ('C', '2021-01-07', '3');


SELECT * FROM Sales;

SELECT * FROM menu;

SELECT * FROM members;


--Q.1. What is the total amount each customer spent at the restaurant?


SELECT customer_id, CONCAT('$',sum(price)) AS Total_spent
FROM menu INNER JOIN sales
ON menu.product_id = sales.product_id
GROUP BY 1
ORDER BY 1;
	

-- Q.2 How many days has each customer visited the restaurant?

SELECT customer_id,COUNT(DISTINCT order_date) AS visit_count
FROM sales
GROUP BY 1
ORDER BY 1;

--  Q.3. What was the first item from the menu purchased by each customer?

WITH order_info_CTE AS
	(SELECT customer_id, 
	 		order_date,
	 		product_name,
	 		DENSE_RANK() OVER (PARTITION BY sales.customer_id ORDER BY sales.order_date) AS rank_num
	 FROM sales JOIN menu 
	 ON sales.product_id = menu.product_id)
	 
SELECT customer_id, product_name
	FROM order_info_CTE
	WHERE rank_num = 1
GROUP BY 1,2;	
	 
SELECT customer_id, 
	 		order_date,
	 		product_name,
	 		DENSE_RANK() OVER (PARTITION BY sales.customer_id ORDER BY sales.order_date) AS rank_num
	 FROM sales JOIN menu 
	 ON sales.product_id = menu.product_id;
	 
	 
-- Q.4 What is the most purchased item on the menu and how many times was it purchased by all customers?

SELECT m.product_name AS most_purchased_item, COUNT(s.product_id) AS TOTAL_COUNT
FROM menu m INNER JOIN sales s
ON m.product_id = s.product_id
GROUP BY 1
ORDER BY TOTAL_COUNT DESC
LIMIT 1;

-- Q.5 Which item was the most popular for each customer ?

WITH order_count_info AS (SELECT 	customer_id,
		product_name,
		COUNT(product_name) AS order_count,
	RANK() OVER(PARTITION BY customer_id ORDER BY COUNT(product_name) DESC ) AS rank_num 
FROM menu INNER JOIN sales 
ON menu.product_id = sales.product_id
GROUP BY 1,2
ORDER BY 1,2)

SELECT customer_id, product_name
 		FROM order_count_info
		WHERE rank_num  = 1;

-- Q.6 Which item was purchased first by the customer after they became a member?

WITH CTM_Q6 AS
	(SELECT S.CUSTOMER_ID,
			MENU.PRODUCT_NAME,
			S.ORDER_DATE,
			M.JOIN_DATE,
			S.PRODUCT_ID,
			RANK() OVER (PARTITION BY S.CUSTOMER_ID ORDER BY S.ORDER_DATE) AS RANK_NUM
	 
		FROM SALES S
		INNER JOIN MEMBERS M ON S.CUSTOMER_ID = M.CUSTOMER_ID
		INNER JOIN MENU ON S.PRODUCT_ID = MENU.PRODUCT_ID
		WHERE S.ORDER_DATE > M.JOIN_DATE)
		
SELECT CUSTOMER_ID,
	PRODUCT_NAME,
	ORDER_DATE
FROM CTM_Q6
WHERE RANK_NUM = 1;


-- Q.7 Which item was purchased just before the customer became a member?

WITH CTM_Q7 AS
	(SELECT S.CUSTOMER_ID,
MENU.PRODUCT_NAME,
S.ORDER_DATE,
M.JOIN_DATE,
S.PRODUCT_ID, RANK() OVER (PARTITION BY S.CUSTOMER_ID ORDER BY S.ORDER_DATE DESC) AS RANK_NUM
		FROM SALES S
		INNER JOIN MEMBERS M ON S.CUSTOMER_ID = M.CUSTOMER_ID
		INNER JOIN MENU ON S.PRODUCT_ID = MENU.PRODUCT_ID
		WHERE S.ORDER_DATE < M.JOIN_DATE
	)
		
SELECT CUSTOMER_ID,
	PRODUCT_NAME
FROM CTM_Q7
WHERE RANK_NUM = 1;


	
--Q.8 What is the total items and amount spent for each member before they became a member?	

SELECT S.CUSTOMER_ID,
	COUNT(MENU.PRODUCT_NAME) AS TOTAL_ITEMS,
	SUM(MENU.PRICE) AS AMOUNT_SPENT
FROM SALES S
INNER JOIN MEMBERS M ON S.CUSTOMER_ID = M.CUSTOMER_ID
INNER JOIN MENU ON S.PRODUCT_ID = MENU.PRODUCT_ID
WHERE S.ORDER_DATE < M.JOIN_DATE
GROUP BY S.CUSTOMER_ID
ORDER BY S.CUSTOMER_ID;

-- Q.9 If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?


WITH CTM_Q9 AS
	(SELECT S.CUSTOMER_ID,
			S.PRODUCT_ID,
			M.PRODUCT_NAME,
			M.PRICE,
			CASE
							WHEN M.PRODUCT_NAME = 'sushi' THEN PRICE * 2 * 10
							ELSE PRICE * 10
			END AS POINTS
		FROM SALES S
		INNER JOIN MENU M ON S.PRODUCT_ID = M.PRODUCT_ID
		ORDER BY 1,2)
SELECT CUSTOMER_ID,
	SUM(POINTS) AS TOTAL_POINTS
FROM CTM_Q9
GROUP BY 1;

/*
	Q.10 In the first week after a customer joins the program (including their join date) they earn 2x points 
	on all items,not just sushi - how many points do customer A and B have at the end of January?
*/
WITH CTM_q10 AS
(SELECT 	s.customer_id,
		s.product_id,
		menu.product_name,
		menu.price, 
		s.order_date,
		m.join_date, DATE_PART('week',join_date) AS week_number,
		CASE
							WHEN DATE_PART('week',join_date) = 1 THEN PRICE * 2 * 10
							WHEN DATE_PART('week',join_date) != 1 THEN
									CASE
										WHEN menu.PRODUCT_NAME = 'sushi' THEN PRICE * 2 * 10
										ELSE PRICE * 10
									END 
							ELSE PRICE * 10	
		END AS POINTS
		FROM sales s INNER JOIN menu ON s.product_id = menu.product_id
		INNER JOIN members m ON s.customer_id = m.customer_id
		WHERE order_date >= join_date
 )
 
 SELECT customer_id, SUM(points)
 FROM ctm_q10
 GROUP BY 1
 ORDER BY 1;
 
 
 /*
 --------------------Bonus Questions------------------
 The following questions are related creating basic data tables that Danny and his team can use 
 to quickly derive insights without needing to join the underlying tables using SQL.
*/

SELECT S.CUSTOMER_ID,
	S.ORDER_DATE,
	MENU.PRODUCT_NAME,
	MENU.PRICE,
	CASE
		WHEN S.ORDER_DATE >= M.JOIN_DATE THEN 'Y'
		ELSE 'N'
	END AS MEMBER
FROM SALES S
LEFT JOIN MENU ON S.PRODUCT_ID = MENU.PRODUCT_ID
LEFT JOIN MEMBERS M ON S.CUSTOMER_ID = M.CUSTOMER_ID
ORDER BY 1,2,3;

/*
Danny also requires further information about the ranking of customer products, 
but he purposely does not need the ranking for non-member purchases so he expects null ranking 
values for the records when customers are not yet part of the loyalty program.
*/

SELECT S.CUSTOMER_ID,
	S.ORDER_DATE,
	MENU.PRODUCT_NAME,
	MENU.PRICE,
	CASE
		WHEN S.ORDER_DATE >= M.JOIN_DATE THEN 'Y'
		ELSE 'N'
	END AS MEMBER
FROM SALES S
LEFT JOIN MENU ON S.PRODUCT_ID = MENU.PRODUCT_ID
LEFT JOIN MEMBERS M ON S.CUSTOMER_ID = M.CUSTOMER_ID
ORDER BY 1,2,3;

/*
Danny also requires further information about the ranking of customer products, 
but he purposely does not need the ranking for non-member purchases 
so he expects null ranking values for the records when customers are not yet part of the loyalty program.
*/


WITH cte_bonus AS (
SELECT S.CUSTOMER_ID,
	S.ORDER_DATE,
	MENU.PRODUCT_NAME,
	MENU.PRICE,
	CASE
		WHEN S.ORDER_DATE >= M.JOIN_DATE THEN 'Y'
		ELSE 'N'
	END AS MEMBER
FROM SALES S
LEFT JOIN MENU ON S.PRODUCT_ID = MENU.PRODUCT_ID
LEFT JOIN MEMBERS M ON S.CUSTOMER_ID = M.CUSTOMER_ID
ORDER BY 1,2,3
)

SELECT customer_id,
	   order_date,
	   product_name,
	   price,
	   member,
	   CASE 
	   	WHEN member = 'Y' THEN RANk() OVER (PARTITION BY customer_id ORDER BY 
								CASE WHEN member = 'Y' THEN order_date END)
		ELSE NULL
		END AS ranking	 
	  FROM cte_bonus
	  ORDER BY 1,2;



