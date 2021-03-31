SELECT u.DisplayName, b.Name 
FROM users as u, badges as b 
WHERE b.id = u.id 