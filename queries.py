filter_comments_by_id_maria = """SELECT
	Id,
	Score
FROM 
	comments
WHERE
	Id BETWEEN '100' AND '200'
"""

def filter_comments_by_id_mongo(db):
   return db.comments.find({
      "$and": [{
      "Id":{ "$gte" : 100  }
   },{ "Id":{ "$lte" :  200 }
      }]
   },{
      "Id": 1,
      "Score": 1
   })

user_badges_maria = """SELECT 
   u.DisplayName, 
   b.Name 
FROM 
   users as u,
   badges as b 
WHERE 
   b.id = u.id 
"""



def user_badges_mongo(db):
   return None

count_votes_bounty = """SELECT 
    COUNT(BountyAmount)
FROM
	votes"""

def count_votes_bounty(db):
   return None



filter_users_by_upvote_maria = """SELECT 
    Id, 
    UpVotes
FROM 
    users
WHERE 
    UpVotes > 100"""

def filter_users_by_upvote_mongo(db):
   return db.users.find({
      "UpVotes" : { "$gt" :  100 }
      },{
         "Id": 1,
         "UpVotes": 1
      })

sort_posts_by_viewcount_maria = """SELECT
     Id,
     ViewCount
FROM
     posts
ORDER BY
     ViewCount, Id"""

def sort_posts_by_viewcount_mongo(db):
   return db.posts.aggregate([
    {"$sort": {"ViewCount": 1}},
    {"$project": {"ViewCount": 1 ,  "Id": 1, "_id": 0}},
    ], allowDiskUse=True)

outer_join_tags_count_maria = """SELECT 
	Count
FROM 
	tags
LEFT JOIN 
	posts
  ON 
  tags.Count = posts.AnswerCount"""

def outer_join_tags_count_mongo(db):
   return db.tags.aggregate([
      {
         "$lookup":{
            "from":"posts",
            "localField":"Count",
            "foreignField":"AnswerCount",
            # "pipeline": [
            #    { "$project": { "Count": 1}}
            #],
            "as":"Answers"
         }
      },
      {
      "$replaceRoot": {"newRoot": { "$mergeObjects": [ { "$arrayElemAt": [ "Answers", 0]}, "$$ROOT"]}}
      },
      {
         "$project":{
            "_id": 0,
            "Count": 1
         }
      }
      ])

outer_join_comments_users_maria = """SELECT 
    users.DisplayName, comments.`Text` 
FROM
    comments LEFT JOIN users 
  ON
    users.Id = comments.UserId"""

def outer_join_comments_users_mongo(db):
   return db.comments.aggregate([
      {
         "$lookup":{
            "from":"db.users",
            "localField":"UserId",
            "foreignField":"Id",
            "as":"users_comments"
         }
      },
      {
      "$replaceRoot": {"newRoot": { "$mergeObjects": [ { "$arrayElemAt": [ "$users_comments", 0]}, "$$ROOT"]}}
      },
      {
         "$project":{
            "_id": 0,
            "Text": 1,
            "DisplayName" : 1
         }
      }
      ])

update_users_name_maria = """UPDATE 
	posts
SET 
	OwnerDisplayName = 'Anonymous'
WHERE 
	OwnerDisplayName = 'user28'"""

def update_users_name_mongo(db):
   return db.posts.update({"OwnerDisplayName": "user28"}, {"$set": {'UserDisplayName': 'Anonymous'}})


average_post_fav_count_maria = """SELECT 
	AVG(FavoriteCount)
FROM 
	posts"""

def average_post_fav_count_mongo(db):
   return db.posts.aggregate(
      [
      {
         "$group":
         {
            "_id": "Id",
            "AverageValue": { "$avg": "$FavoriteCount" }
         }
      }
      ])


count_votes_bounty_maria = """SELECT 
    COUNT(BountyAmount)
FROM
	votes"""

def count_votes_bounty_mongo(db):
   return None


select_owner_not_null_maria = """SELECT 
	OwnerDisplayName
FROM 
	posts
WHERE 
	OwnerDisplayName IS NOT NULL"""

def select_owner_not_null_mongo(db):
   return None

select_tag_max_count_maria = """SELECT 
	MAX(Count)
FROM 
	tags"""

def select_tag_max_count_mongo(db):
   return db.tags.aggregate([
      {
         "$group":
         {
            "_id": "Id",
            "MaxValue": { "$max": "$Count" }
         }
      },
      {
         "$project":
               {
                  "_id": 0,
                  "Id": 0
               }
      }
      ])

sum_users_downvotes_maria ="""SELECT 
	SUM(DownVotes)
FROM 
	users"""

def sum_users_downvotes_mongo(db):
   return db.users.aggregate([
      {
         "$group":
         {
            "_id": "Id",
            "Sum": { "$sum": "$DownVotes" }
         }
      },
      # {
      #    "$project":
      #          {
      #             "_id": 0,
      #             "Id": 0
      #          }
      # }
])


count_users_by_age_maria = """SELECT 
    Age,
	COUNT(Reputation)
FROM 
	users GROUP BY Age"""

def count_users_by_age_mongo(db):
   return db.users.aggregate(
      [

      {"$group" : {"_id":"$Age", "count":{"$sum":1}}},
      # {"Age":1, "count":1}
      ])

insert_badges_maria = """INSERT INTO 
	badges(Id, userId, Name, Date)
VALUES
	({}, 27, 'Critic', '2013-12-03 11:54:06')
"""

def insert_badges_mongo(db):
   return db.badges.insert_one( { "Id": "233333", "UserId": 27, "Name": "Critic", "Date": '2013-12-03 11:54:06'  } )


delete_user_badges_maria = """DELETE FROM 
	badges
WHERE
	userId = 27"""

def delete_user_badges_mongo(db):
   return db.badges.delete_one(
      { "UserId": 27}
)

drop_badges_maria = """DROP TABLE 
	badges"""

def drop_badges_mongo(db):
   return db.badges.drop()

select_comments_maria = """
SELECT
   *
FROM
   comments
LIMIT 50000
"""

def select_comments_mongo(db,limit=None):
    return db.comments.find().limit(50000)


queries_list = [
   
   { "name" : "filter_comments_by_id",
      "maria" : filter_comments_by_id_maria,
      "mongo": filter_comments_by_id_mongo
   },
   { "name": "filter_users_by_upvote",
      "maria": filter_users_by_upvote_maria,
      "mongo": filter_users_by_upvote_mongo
   },
   { "name": "sort_posts_by_viewcount",
      "maria": sort_posts_by_viewcount_maria,
      "mongo": sort_posts_by_viewcount_mongo
   },
   { "name": "outer_join_tags_count",
      "maria": outer_join_tags_count_maria,
      "mongo": outer_join_tags_count_mongo
   },
   { "name": "outer_join_comments_users",
      "maria": outer_join_comments_users_maria,
      "mongo": outer_join_comments_users_mongo
   },
   { "name": "update_users_name",
      "maria": update_users_name_maria,
      "mongo": update_users_name_mongo
   },
   { "name": "average_post_fav_count",
      "maria": average_post_fav_count_maria,
      "mongo": average_post_fav_count_mongo
   },
   { "name": "count_votes_bounty",
      "maria": count_votes_bounty_maria,
      "mongo": count_votes_bounty_mongo
   },
   { "name": "select_owner_not_null",
      "maria": select_owner_not_null_maria,
      "mongo": select_owner_not_null_mongo
   },
   { "name": "select_tag_max_count",
      "maria": select_tag_max_count_maria,
      "mongo": select_tag_max_count_mongo
   },
   { "name": "sum_users_downvotes",
      "maria": sum_users_downvotes_maria,
      "mongo": sum_users_downvotes_mongo
   },
   { "name": "count_users_by_age",
      "maria": count_users_by_age_maria,
      "mongo": count_users_by_age_mongo
   },
   { "name": "insert_badges",
      "maria": insert_badges_maria,
      "mongo": insert_badges_mongo
   },
   { "name": "delete_user_badges",
      "maria": delete_user_badges_maria,
      "mongo": delete_user_badges_mongo
   },
   { "name": "drop_badges",
      "maria": drop_badges_maria,
      "mongo": drop_badges_mongo
   },
   { "name": "user_badges",
      "maria": user_badges_maria,
      "mongo": user_badges_mongo
   },
   { "name": "select_comments",
      "maria": select_comments_maria,
      "mongo": select_comments_mongo
   },
]

def get_query_by_name(queries_list, name):
   for query in queries_list:
      if query["name"] == name:
         return query
