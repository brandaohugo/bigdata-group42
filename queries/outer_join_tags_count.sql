SELECT 
	Count
FROM 
	tags
LEFT JOIN 
	posts
  ON 
  tags.Count = posts.AnswerCount