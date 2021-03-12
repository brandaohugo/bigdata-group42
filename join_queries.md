## Show the badges of each user

SELECT u.DisplayName, b.Name 
FROM users as u, badges as b 
WHERE b.id = u.id 
LIMIT 1000;

Show Posts with comments and scores

SELECT p.Id, p.Title, p.Body, c.Score, c.Text
FROM posts as p, comments as c
WHERE p.Id = c.PostId
ORDER BY p.Id
LIMIT 10000;
