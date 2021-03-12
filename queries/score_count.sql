SELECT c.Score, count(c.Score) as counts
FROM posts as p, comments as c
WHERE p.Id = c.PostId
GROUP BY c.Score;