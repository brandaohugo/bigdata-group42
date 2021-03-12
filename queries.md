# Experiment Queries

## Stats Dataset

### user_badges (join)
SELECT u.DisplayName, b.Name 
FROM users as u, badges as b 
WHERE b.id = u.id 
LIMIT 1000;

### posts_comments_scores (join + sort)

SELECT p.Id, p.Title, p.Body, c.Score, c.Text
FROM posts as p, comments as c
WHERE p.Id = c.PostId
ORDER BY p.Id
LIMIT 10000;


### score_counts (join + aggregation)

SELECT c.Score, count(c.Score) as counts
FROM posts as p, comments as c
WHERE p.Id = c.PostId
GROUP BY c.Score;


## TPC-D Dataset