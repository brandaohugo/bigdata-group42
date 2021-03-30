SELECT 
    p.Id,
    p.Title,
    p.Body,
    c.Score,
    c.Text
FROM
    posts as p, 
    comments as c
WHERE 
    p.Id = c.PostId
ORDER BY 
    p.Id