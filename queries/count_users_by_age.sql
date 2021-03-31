SELECT 
    Age,
	COUNT(Reputation)
FROM 
	users GROUP BY Age;