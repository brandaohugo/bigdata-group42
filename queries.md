## Join Queries

### user_badges
SELECT u.DisplayName, b.Name 
FROM users as u, badges as b 
WHERE b.id = u.id 
LIMIT 1000;

### posts_comments_scores

SELECT p.Id, p.Title, p.Body, c.Score, c.Text
FROM posts as p, comments as c
WHERE p.Id = c.PostId
ORDER BY p.Id
LIMIT 10000;


## Aggregation Queries

### score_counts

SELECT c.Score, count(c.Score) as counts
FROM posts as p, comments as c
WHERE p.Id = c.PostId
GROUP BY c.Score;