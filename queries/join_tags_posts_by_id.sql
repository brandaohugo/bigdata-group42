SELECT
	tags.Count,
    posts.AnswerCount
FROM
    tags JOIN posts USING (Id)