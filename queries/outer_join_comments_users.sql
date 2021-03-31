SELECT 
    users.DisplayName, comments.`Text` 
FROM
    comments LEFT JOIN users 
  ON
    users.Id = comments.UserId